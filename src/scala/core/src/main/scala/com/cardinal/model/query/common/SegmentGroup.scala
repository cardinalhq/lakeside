package com.cardinal.model.query.common

import org.joda.time.DateTime

case class SegmentGroup(startTs: Long, endTs: Long, segments: List[SegmentInfo]) {

  override def toString: String = {
    s"${new DateTime(startTs)} - ${new DateTime(endTs)}/${segments.size}"
  }
}
