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

package com.cardinal.eval

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.cardinal.instrumentation.Metrics.count
import com.cardinal.logs.LogCommons.{COUNT, MAX, MIN, SUM}
import com.cardinal.utils.ast.BaseExpr.{ddSketchFromBytes, hllSketchFromBytes, CARDINALITY_ESTIMATE_AGGREGATION}
import com.cardinal.utils.ast.SketchTags.{DD_SKETCH_TYPE, HLL_SKETCH_TYPE}
import com.cardinal.utils.ast.{BaseExpr, SketchGroup, SketchInput}
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.Clock
import org.apache.datasketches.hll.Union
import org.slf4j.LoggerFactory

trait SketchMerger {
  def mergeSketch(sketchInput: SketchInput): Unit

  def datapoints: List[SketchInput]

  def mergeSketches(
    existingSketchBytes: Array[Byte],
    incomingSketchBytes: Array[Byte],
    sketchType: String
  ): Array[Byte] = {
    sketchType match {
      case DD_SKETCH_TYPE =>
        val existingSketch = ddSketchFromBytes(existingSketchBytes)
        existingSketch.mergeWith(ddSketchFromBytes(incomingSketchBytes))
        existingSketch.serialize().array()

      case HLL_SKETCH_TYPE =>
        val existingSketch = hllSketchFromBytes(existingSketchBytes)
        val incomingSketch = hllSketchFromBytes(incomingSketchBytes)
        val union = new Union()
        union.update(incomingSketch)
        union.update(existingSketch)
        union.toCompactByteArray
    }
  }
}
private class SimpleSketchMerger(var initDataPoint: SketchInput) extends SketchMerger {
  private var mergedSketch = initDataPoint.sketchTags.sketch match {
    case Left(sketch) => Left(sketch)
    case Right(map)   => Right(map)
  }

  def mergeSketch(sketchInput: SketchInput): Unit = {
    sketchInput.sketchTags.sketch match {
      case Left(incomingSketch) =>
        mergedSketch = mergedSketch.left.map(
          existingSketchBytes =>
            mergeSketches(
              existingSketchBytes = existingSketchBytes,
              incomingSketchBytes = incomingSketch,
              sketchType = sketchInput.sketchTags.sketchType
          )
        )
      case Right(incomingMap) =>
        mergedSketch.map { existingMap =>
          val keys = (existingMap.keys ++ incomingMap.keys).toSet
          val mergedMap = keys.map {
            case key @ (SUM | COUNT) => key -> (existingMap.getOrElse(key, 0.0) + incomingMap.getOrElse(key, 0.0))
            case key @ MIN =>
              key -> math.min(
                existingMap.getOrElse(key, Double.MaxValue),
                incomingMap.getOrElse(key, Double.MaxValue)
              )
            case key @ MAX =>
              key -> math.max(
                existingMap.getOrElse(key, Double.MinValue),
                incomingMap.getOrElse(key, Double.MinValue)
              )
          }.toMap
          mergedSketch = Right(mergedMap)
        }
    }
  }

  override def datapoints: List[SketchInput] =
    List(initDataPoint.copy(sketchTags = initDataPoint.sketchTags.copy(sketch = mergedSketch match {
      case Left(sketch) => Left(sketch)
      case Right(value) => Right(value)
    })))
}

private class GroupBySketchMerger extends SketchMerger {
  private val aggregators =
    scala.collection.mutable.AnyRefMap.empty[Map[String, Any], SimpleSketchMerger]

  override def mergeSketch(sketchInput: SketchInput): Unit = {
    aggregators.get(sketchInput.sketchTags.tags) match {
      case Some(aggr) => aggr.mergeSketch(sketchInput)
      case None       => aggregators.put(sketchInput.sketchTags.tags, new SimpleSketchMerger(sketchInput))
    }
  }

  override def datapoints: List[SketchInput] = aggregators.flatMap(aggr => aggr._2.datapoints).toList
}

class TimeGroupedSketchAggregator(numBuffers: Int = 2, baseExprLookup: SketchInput => Option[BaseExpr])
    extends GraphStage[FlowShape[List[SketchInput], List[SketchGroup]]] {
  private val logger = LoggerFactory.getLogger(getClass)
  private val in = Inlet[List[SketchInput]](s"${getClass.getSimpleName}.in")
  private val out = Outlet[List[SketchGroup]](s"${getClass.getSimpleName}.out")

  private type AggrMap = java.util.HashMap[BaseExpr, SketchMerger]
  private val clock = Clock.SYSTEM

  override val shape: FlowShape[List[SketchInput], List[SketchGroup]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private val buf = new Array[AggrMap](numBuffers)
      buf.indices.foreach { i =>
        buf(i) = new AggrMap
      }

      private val timestamps = new Array[Long](numBuffers)

      private var step = -1L
      private var cutoffTime = 0L

      private var pending: List[SketchGroup] = Nil

      private def findBuffer(t: Long): Int = {
        var i = 0
        var min = 0
        while (i < timestamps.length) {
          if (timestamps(i) == t) return i
          if (i > 0 && timestamps(i) < timestamps(i - 1)) min = i
          i += 1
        }
        -min - 1
      }

      /**
        * Add a value to the aggregate or create a new aggregate initialized to the provided
        * value. Heartbeat datapoints will be ignored as they are just used to trigger flushing
        * of the time group.
        */
      private def aggregate(i: Int, v: SketchInput): Unit = {
        baseExprLookup.apply(v) match {
          case Some(baseExpr) =>
            val aggr = buf(i).get(baseExpr)
            if (aggr == null) {
              val hasGroupBys = baseExpr.chartOpts.map(copts => copts.groupBys).getOrElse(List())
              buf(i).put(
                baseExpr,
                if (hasGroupBys.nonEmpty && v.sketchTags.sketchType != CARDINALITY_ESTIMATE_AGGREGATION) {
                  val gMerger = new GroupBySketchMerger
                  gMerger.mergeSketch(v)
                  gMerger
                } else new SimpleSketchMerger(v)
              )
            } else {
              aggr.mergeSketch(v)
            }
          case None =>
            logger.error(s"Error: BaseExpr not found for SketchInput ${v.evalConfigId} ${Json.encode(v.sketchTags.tags)}")
        }
      }

      private def toTimeGroup(ts: Long, aggrMap: AggrMap): SketchGroup = {
        import scala.jdk.CollectionConverters._
        val dataPointsByBaseExpr = aggrMap.asScala.map {
          case (expr, aggr) => expr -> aggr.datapoints
        }.toMap

        SketchGroup(ts, dataPointsByBaseExpr)
      }

      /**
        * Push the most recently completed time group to the next stage and reset the buffer
        * so it can be used for a new time window.
        */
      private def flush(i: Int): Option[SketchGroup] = {
        val t = timestamps(i)
        val group = if (t > 0) Some(toTimeGroup(t, buf(i))) else None
        cutoffTime = t
        buf(i) = new AggrMap
        group
      }

      override def onPush(): Unit = {
        val builder = List.newBuilder[SketchGroup]
        grab(in).foreach { v =>
          val t = v.timestamp
          val now = clock.wallTime()
          step = v.frequency
          if (t > now) {
            count("droppedRecords", 1.0, "type:future")
          } else if (t <= cutoffTime) {
//            logger.info(s"Dropping old records for ${v.evalConfigId}/${v.baseExprId} at time = ${new DateTime(t)}, cutOffTime = ${new DateTime(cutoffTime)}")
            count("droppedRecords", 1.0, "type:old", s"configId:${v.evalConfigId}")
          } else {
            val i = findBuffer(t)
            if (i >= 0) {
              aggregate(i, v)
            } else {
              val pos = -i - 1
              builder ++= flush(pos)
              aggregate(pos, v)
              timestamps(pos) = t
            }
          }
        }
        val groups = builder.result()
        if (groups.isEmpty)
          pull(in)
        else
          push(out, groups)
      }

      override def onPull(): Unit = {
        if (isClosed(in))
          flushPending()
        else
          pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        val groups = buf.indices.map { i =>
          toTimeGroup(timestamps(i), buf(i))
        }.toList
        pending = groups.filter(_.timestamp > 0).sortWith(_.timestamp < _.timestamp)
        flushPending()
      }

      private def flushPending(): Unit = {
        if (pending.nonEmpty && isAvailable(out)) {
          push(out, pending)
          pending = Nil
        }
        if (pending.isEmpty) {
          completeStage()
        }
      }
      setHandlers(in, out, this)

    }

}
