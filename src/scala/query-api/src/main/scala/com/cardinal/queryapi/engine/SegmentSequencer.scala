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

import java.util.Base64

object SegmentSequencer {
  private val tsUri = "/api/internal/timeseries"

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
      decoded("type").str match {
        case "exemplar" =>
          val timestamp = decoded("timestamp").num.toLong
          val value = decoded("value").num
          val tags = decoded("tags").obj.map(e => e._1 -> e._2.str).toMap
          val point = DataPoint(timestamp = timestamp, value = value, tags = tags)
          Left(point)
        case "sketch" =>
          val timestamp = decoded("timestamp").num.toLong
          val tags = decoded("tags").obj.map(e => e._1 -> e._2.str).toMap
          val sketchType = decoded("sketchType").str
          val sketch = sketchType match {
            case HLL_SKETCH_TYPE | DD_SKETCH_TYPE =>
              val sketchBytes = decoded("sketch").str
              val bytes = Base64.getDecoder.decode(sketchBytes)
              Left(bytes)
            case MAP_SKETCH_TYPE => Right(decoded("sketch").obj.map(e => e._1 -> e._2.num).toMap)
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
