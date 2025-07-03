package com.cardinal.model.query.common

import com.cardinal.model.SegmentRequest
import org.joda.time.DateTime
import ujson.Value

object SegmentInfo {

  def fromJson(segmentInfoValue: Value): SegmentInfo = {
    SegmentInfo(
      dateInt = segmentInfoValue("dateInt").str,
      hour = segmentInfoValue("hour").str,
      segmentId = segmentInfoValue("segmentId").str,
      sealedStatus = segmentInfoValue("sealedStatus").bool,
      startTs = segmentInfoValue("startTs").str.toLong,
      endTs = segmentInfoValue("endTs").str.toLong,
      exprId = segmentInfoValue("exprId").str,
      dataset = segmentInfoValue("dataset").str,
      bucketName = segmentInfoValue("bucketName").str,
      customerId = segmentInfoValue("customerId").str,
      collectorId = segmentInfoValue("collectorId").str,
      frequency = segmentInfoValue("frequency").str.toLong
    )
  }

  def toJson(segmentInfo: SegmentInfo): Value = {
    ujson.Obj(
      "dateInt"      -> segmentInfo.dateInt,
      "hour"         -> segmentInfo.hour,
      "segmentId"    -> segmentInfo.segmentId,
      "sealedStatus" -> segmentInfo.sealedStatus,
      "startTs"      -> segmentInfo.startTs,
      "endTs"        -> segmentInfo.endTs,
      "exprId"       -> segmentInfo.exprId,
      "dataset"      -> segmentInfo.dataset,
      "bucketName"   -> segmentInfo.bucketName,
      "customerId"   -> segmentInfo.customerId,
      "collectorId"  -> segmentInfo.collectorId,
      "frequency"    -> segmentInfo.frequency
    )
  }
}

case class SegmentInfo(
  dateInt: String,
  hour: String,
  segmentId: String,
  sealedStatus: Boolean,
  startTs: Long,
  endTs: Long,
  exprId: String,
  dataset: String,
  bucketName: String,
  customerId: String,
  collectorId: String,
  cName: String = "",
  frequency: Long
) {

  def id: String = s"${segmentId}_${customerId}_${collectorId}_${bucketName}_$exprId"
  override def equals(obj: Any): Boolean = id == obj.asInstanceOf[SegmentInfo].id

  override def hashCode(): Int = id.hashCode

  def toSegmentRequest(queryTags: Map[String, Any]): SegmentRequest = {
    SegmentRequest(
      hour = hour,
      dateInt = dateInt,
      segmentId = segmentId,
      sealedStatus = sealedStatus,
      queryTags = queryTags,
      stepInMillis = frequency,
      dataset = dataset,
      customerId = customerId,
      collectorId = collectorId,
      bucketName = bucketName,
      cName = cName,
      startTs = startTs,
      endTs = endTs
    )
  }

  override def toString: String = {
    s"$segmentId/$dateInt/$hour/${new DateTime(startTs)} - ${new DateTime(endTs)}"
  }
}
