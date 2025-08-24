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

package com.cardinal.queryapi.engine

import akka.stream._
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.cardinal.auth.AuthToken
import com.cardinal.model.query.common.TagDataType
import com.cardinal.model.{DataPoint, PostPushDownProcessor, PushDownRequest, SegmentRequest}
import com.cardinal.queryapi.engine.QueryEngineV2.sourceFromRemote
import com.cardinal.utils.Commons.DEFAULT_CUSTOMER_ID
import com.cardinal.utils.ast.SketchTags.{DD_SKETCH_TYPE, HLL_SKETCH_TYPE, MAP_SKETCH_TYPE}
import com.cardinal.utils.ast.{BaseExpr, SketchInput, SketchTags}

object SegmentSequencer {
  private val tsUri = "/api/internal/timeseries"

  import scala.util.Try

  private def asDouble(v: ujson.Value): Double = v match {
    case ujson.Num(n) => n
    case ujson.Str(s) =>
      s match {
        case "NaN" | "nan" => Double.NaN
        case "Infinity" | "+Infinity" => Double.PositiveInfinity
        case "-Infinity" => Double.NegativeInfinity
        case other => Try(other.toDouble).getOrElse(Double.NaN)
      }
    case _ => Double.NaN
  }

  private def asLong(v: ujson.Value): Long = v match {
    case ujson.Num(n) => n.toLong
    case ujson.Str(s) => Try(s.toLong).getOrElse(0L)
    case _ => 0L
  }

  def allSources(
                  queryId: String,
                  baseExpr: BaseExpr,
                  segmentRequests: List[SegmentRequest],
                  segmentCacheManager: SegmentCacheManager,
                  reverseSort: Boolean = false,
                  isTagQuery: Boolean = false,
                  tagDataType: Option[TagDataType],
                  postPushDownProcessor: Option[PostPushDownProcessor] = None
                )(implicit mat: Materializer): Seq[Source[Either[DataPoint, SketchInput], Any]] = {
    val customerId = segmentRequests.headOption.map(_.customerId).getOrElse(DEFAULT_CUSTOMER_ID)

    def decode(baseExpr: BaseExpr, bs: ByteString): Either[DataPoint, SketchInput] = {
      val json = bs.utf8String
      val decoded = ujson.read(json).obj
      val tags = decoded("tags").obj.map { case (k, v) => k -> v.str }.toMap

      decoded("type").str match {
        case "exemplar" =>
          val timestamp = asLong(decoded("timestamp"))
          val value = asDouble(decoded("value")) // <-- was .num
          Left(DataPoint(timestamp = timestamp, value = value, tags = tags))

        case "sketch" =>
          val timestamp = asLong(decoded("timestamp"))
          val sketchType = decoded("sketchType").str

          val sketch = sketchType match {
            case HLL_SKETCH_TYPE | DD_SKETCH_TYPE =>
              val b64 = decoded("sketch").str
              val bytes = java.util.Base64.getDecoder.decode(b64)
              Left(bytes)

            case MAP_SKETCH_TYPE =>
              // tolerate "NaN" etc for per-key values
              val m = decoded("sketch").obj.map { case (k, v) => k -> asDouble(v) }.toMap
              Right(m)
          }

          Right(
            SketchInput(
              customerId = customerId,
              baseExprHashCode = baseExpr.id,
              timestamp = timestamp,
              sketchTags = SketchTags(tags = tags, sketchType = sketchType, sketch = sketch)
            )
          )
      }
    }

    val groupedByCName = segmentRequests.filter(_.cName.nonEmpty).groupBy(_.cName)

    val processor = postPushDownProcessor match {
      case Some(ppd) => Some(ppd)
      case None =>
        baseExpr.chartOpts match {
          case Some(cOpts) =>
            cOpts.fieldName.map { field =>
              PostPushDownProcessor(resetValueToField = Some(field))
            }
          case None => None
        }
    }
    if (groupedByCName.nonEmpty) {
      groupedByCName.map {
        case (cName, segments) =>
          val uri = s"$tsUri?queryId=$queryId"
          sourceFromRemote(
            uri = uri,
            func = (bs: ByteString) => decode(baseExpr, bs),
            ip = cName,
            pushDownRequest = PushDownRequest(
              baseExpr = baseExpr,
              segmentRequests = segments,
              processor = processor,
              reverseSort = reverseSort,
              isTagQuery = isTagQuery,
              tagDataType = tagDataType,
            ),
            useHttps = true,
            headers = AuthToken.getAuthHeader(customerId)
          )(mat.system)
      }.toList
    } else {
      val groupedByPod = segmentCacheManager.getGroupedByQueryWorkerPod(segmentRequests)
      // Get cached sources from query workers
      groupedByPod
        .filter(_._2.nonEmpty)
        .map { grouped =>
          val pod = grouped._1
          val segments = grouped._2
          sourceFromRemote(
            uri = s"$tsUri?queryId=$queryId",
            func = (bs: ByteString) => decode(baseExpr, bs),
            ip = pod.ip,
            pushDownRequest = PushDownRequest(
              baseExpr = baseExpr,
              segmentRequests = segments,
              processor = processor,
              reverseSort = reverseSort,
              isTagQuery = isTagQuery,
              tagDataType = tagDataType
            )
          )(mat.system)
        }
        .toList
    }
  }
}
