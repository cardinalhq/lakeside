/*
 * Copyright (C) 2025 CardinalHQ, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.cardinal.utils

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.cardinal.model.{DataPoint, PushDownRequest}
import com.cardinal.utils.Commons.{DEFAULT_CUSTOMER_ID, METRICS, NAME}
import com.cardinal.utils.ast.BaseExpr.CARDINALITY_ESTIMATE_AGGREGATION
import com.cardinal.utils.ast.SketchTags.{DD_SKETCH_TYPE, MAP_SKETCH_TYPE}
import com.cardinal.utils.ast._

class PushDownAggregatorStage(pr: PushDownRequest, stepInMillis: Long)
    extends GraphStage[FlowShape[DataPoint, Either[Iterable[DataPoint], Iterable[SketchInput]]]] {

  private val in: Inlet[DataPoint] = Inlet(s"$getClass.in")
  private val out: Outlet[Either[Iterable[DataPoint], Iterable[SketchInput]]] = Outlet(s"$getClass.out")

  private val groupByKeysExtractor = (tags: Map[String, String]) => {
    val keyMap = Map.newBuilder[String, String]
    pr.groupBys.map(g => g -> tags.get(g)).map {
      case (key, Some(v)) => keyMap += key -> v
      case _              =>
    }
    keyMap.result()
  }
  private val rollupAggregation: Option[String] = pr.rollupAgg
  private val exemplarsOnly = rollupAggregation.isEmpty && pr.globalAgg.isEmpty
  private val needsPercentileAggregation = rollupAggregation.exists(ra => ra.startsWith("p"))
  private val needsCESAggregation = rollupAggregation.exists(ra => ra.contains(CARDINALITY_ESTIMATE_AGGREGATION))

  override def shape: FlowShape[DataPoint, Either[Iterable[DataPoint], Iterable[SketchInput]]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    new GraphStageLogic(shape) with InHandler with OutHandler {

      private val customerId = pr.segmentRequests.headOption.map(_.customerId).getOrElse(DEFAULT_CUSTOMER_ID)
      private var currentModuloTs: Long = -1
      private val ddSketchAggregators = collection.mutable.AnyRefMap.empty[Map[String, String], DDSketchAggregator]
      private var hllAggregatorOpt: Option[HLLAggregator] = None

      override def onPush(): Unit = {
        val datapoint = grab(in)
        val isEventsDataset = pr.segmentRequests.headOption.exists(_.dataset != METRICS)
        val moduloTs = if (isEventsDataset && pr.globalAgg.nonEmpty) {
          datapoint.timestamp - datapoint.timestamp % stepInMillis
        } else {
          datapoint.timestamp
        }

        // If exemplars only, push incoming datapoint without any aggregation
        if (exemplarsOnly) {
          push(out, Left(List(datapoint)))
        } else if (needsPercentileAggregation) {
          if (currentModuloTs == -1 || currentModuloTs == moduloTs) {
            // If in the current time step, update percentile aggregator and move on.
            updateDDSketchAggregator(datapoint)
            if (!hasBeenPulled(in)) pull(in)
          } else {
            // Else, flush the computed percentile aggregators
            val dataSketches = toDDSketches
            ddSketchAggregators.clear()
            push(out, Right(dataSketches))
            // Include the incoming datapoint in the next time step.
            updateDDSketchAggregator(datapoint)
          }
        } else if (needsCESAggregation) {
          if (currentModuloTs == -1 || currentModuloTs == moduloTs) {
            // If in the current time step, update HLL aggregator and move on.
            updateHLLAggregator(datapoint)
            if (!hasBeenPulled(in)) pull(in)
          } else {
            // Else, flush the computed HLL aggregators
            val dataSketches = toHLLDataSketches
            hllAggregatorOpt = None
            push(out, Right(dataSketches))
            // Include the incoming datapoint in the next time step.
            updateHLLAggregator(datapoint)
          }
        } else {
          val dataSketch = SketchInput(
            customerId = customerId,
            timestamp = datapoint.timestamp,
            sketchTags = SketchTags(
              tags = datapoint.tags,
              sketchType = MAP_SKETCH_TYPE,
              Right(Map[String, Double](pr.globalAgg.get -> datapoint.value))
            )
          )
          push(out, Right(List(dataSketch)))
        }
        currentModuloTs = moduloTs
      }

      def toHLLDataSketches: List[SketchInput] = {
        hllAggregatorOpt
          .map(hllAggregator => {
            List(
              SketchInput(
                customerId = customerId,
                timestamp = currentModuloTs,
                sketchTags = hllAggregator.result().head
              )
            )
          })
          .getOrElse(List.empty)
      }

      def toDDSketches: List[SketchInput] = {
        ddSketchAggregators.map {
          case (tags, aggregator) =>
            SketchInput(
              customerId = customerId,
              timestamp = currentModuloTs,
              sketchTags =
                SketchTags(tags = tags, sketchType = DD_SKETCH_TYPE, sketch = aggregator.result().head.sketch)
            )
        }.toList
      }

      private def updateHLLAggregator(datapoint: DataPoint): Unit = {
        if (hllAggregatorOpt.isEmpty) hllAggregatorOpt = Some(new HLLAggregator(pr.groupBys))
        hllAggregatorOpt.foreach(hllAggregator => hllAggregator.update(datapoint.tags, 0.0))
      }

      private def updateDDSketchAggregator(datapoint: DataPoint): Unit = {
        val keyTags = getGroupByKeyTags(datapoint)
        val aggregator = ddSketchAggregators.getOrElseUpdate(keyTags, new DDSketchAggregator(keyTags))
        aggregator.mergeSketch(datapoint.value)
      }

      override def onPull(): Unit = {
        if (isClosed(in))
          flushPending()
        else
          pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        flushPending()
      }

      private def flushPending(): Unit = {
        val available = isAvailable(out)
        if (available && ddSketchAggregators.nonEmpty) {
          val dataSketches = toDDSketches
          ddSketchAggregators.clear()
          push(out, Right(dataSketches))
        } else if (available && hllAggregatorOpt.nonEmpty) {
          val hllBytes = hllAggregatorOpt.get.result()
          hllAggregatorOpt = None
          push(
            out,
            Right(
              List(
                SketchInput(
                  customerId = customerId,
                  timestamp = currentModuloTs,
                  sketchTags = hllBytes.head
                )
              )
            )
          )
        }
        if (ddSketchAggregators.isEmpty && hllAggregatorOpt.isEmpty) {
          completeStage()
        }
      }

      setHandlers(in, out, this)
    }
  }

  private def getGroupByKeyTags(datapoint: DataPoint): Map[String, String] = {
    val keyTags = if (pr.groupBys.isEmpty) {
      val name = datapoint.tags.getOrElse(NAME, "")
      Map[String, String](NAME -> name)
    } else {
      groupByKeysExtractor.apply(datapoint.tags)
    }
    keyTags
  }
}
