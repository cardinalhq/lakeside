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

package com.cardinal.model

import com.cardinal.model.query.common.TagDataType
import com.cardinal.utils.ast.ASTUtils.toBaseExpr
import com.cardinal.utils.ast.BaseExpr
import com.netflix.atlas.json.Json

import scala.collection.mutable

case class PostPushDownProcessor(tagNameCompressionEnabled: Boolean = false, resetValueToField: Option[String] = None)

object PushDownRequest {

  def toJson(pushDownRequest: PushDownRequest): String = {
    val map = mutable.Map.empty[String, AnyRef]
    map += "baseExpr"        -> pushDownRequest.baseExpr.toJsonObj
    map += "segmentRequests" -> Json.decode[List[Map[String, AnyRef]]](Json.encode(pushDownRequest.segmentRequests))
    pushDownRequest.processor.foreach { processor =>
      map += "processor" -> Json.decode[Map[String, AnyRef]](Json.encode(processor))
    }
    map += "reverseSort" -> Boolean.box(pushDownRequest.reverseSort)
    map += "isTagQuery" -> Boolean.box(pushDownRequest.isTagQuery)
    pushDownRequest.tagDataType.foreach { tagDataType =>
      map += "tagDataType" -> Json.decode[Map[String, AnyRef]](Json.encode(tagDataType))
    }
    Json.encode(map.toMap)
  }

  def fromJson(str: String): PushDownRequest = {
    val payload = ujson.read(str).obj
    val baseExpr = toBaseExpr(payload("baseExpr").toString())
    val segmentRequests = Json.decode[List[SegmentRequest]](payload("segmentRequests").toString())
    val processor = payload.get("processor") match {
      case Some(processorObj) => Some(Json.decode[PostPushDownProcessor](processorObj.toString()))
      case None               => None
    }
    val reverseSort = payload("reverseSort").bool
    val isTagQuery = payload("isTagQuery").bool
    var tagDataType: Option[TagDataType] = None
    payload.get("tagDataType").foreach { tagDataTypeObj =>
      tagDataType = Some(Json.decode[TagDataType](tagDataTypeObj.toString()))
    }
    PushDownRequest(baseExpr, segmentRequests, processor, reverseSort, tagDataType = tagDataType, isTagQuery = isTagQuery)
  }
}
case class PushDownRequest(
  baseExpr: BaseExpr,
  segmentRequests: List[SegmentRequest],
  processor: Option[PostPushDownProcessor] = None,
  reverseSort: Boolean = false,
  isTagQuery: Boolean = false,
  tagDataType: Option[TagDataType] = None
) {

  def groupBys: List[String] = {
    if (globalAgg.isEmpty) {
      List.empty
    } else {
      baseExpr.chartOpts.map(_.groupBys).getOrElse(List.empty)
    }
  }

  def globalAgg: Option[String] = baseExpr.chartOpts.map(_.aggregation)

  def rollupAgg: Option[String] = baseExpr.chartOpts.flatMap(_.rollupAggregation)
}

case class SegmentRequest(
  hour: String,
  dateInt: String,
  segmentId: String,
  sealedStatus: Boolean,
  dataset: String,
  queryTags: Map[String, Any],
  stepInMillis: Long,
  customerId: String,
  collectorId: String,
  bucketName: String,
  cName: String,
  startTs: Long,
  endTs: Long
) {
  override def toString: String = {
    s"${this.dateInt}/${this.dataset}/${this.hour}/${this.segmentId}/${this.sealedStatus}"
  }
}
