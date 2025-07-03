package com.cardinal.model.query.common

import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import java.time.Duration

object SequencingStrategy {
  private val logger = LoggerFactory.getLogger(getClass)

  private def logGroups(groups: List[SegmentGroup]): Unit = {
    groups.foreach { group =>
      logger.info(
        s"SegmentGroup = ${new DateTime(group.startTs)}/${new DateTime(group.endTs)}: ${group.segments.size}"
      )
    }
  }

  def computeReplaySequence(
    segments: List[SegmentInfo],
    reverseSort: Boolean = false,
    executionGroupSize: Int,
    startTs: Long,
    endTs: Long,
    frequency: Duration
  ): List[SegmentGroup] = {
    // Uncomment if we want to repro sequencing issues.
//    logger.info(
//      s"segmentList=${Json.encode[List[SegmentInfo]](segments)}, startTs=$startTs, endTs=$endTs, frequency=$frequency," +
//      s"executionGroupSize=$executionGroupSize, reverseSort=$reverseSort"
//    )
    val sealedGroups = computeGroups(
      segments = segments,
      reverseSort = reverseSort,
      queryStartTs = startTs,
      queryEndTs = endTs,
      minGroupSize = executionGroupSize,
      step = frequency
    )

    // For every segment in each segment group, set the startTs and endTs to the startTs and endTs of the group
    // This prevents artificial holes in the data.
    val groups = sealedGroups
      .map(sg => sg.copy(segments = sg.segments.map(s => s.copy(startTs = sg.startTs, endTs = sg.endTs))))
      .sortWith(
        (s1, s2) => if (reverseSort) s1.endTs > s2.endTs else s1.endTs < s2.endTs // reverse sort segment groups if necessary
      )
    logGroups(groups)
    groups
  }

  import java.time.Instant

  // Align the startTs and endTs of every segment into discrete one step boundaries,
  // So, for example S1: (9:16 - 9:19) becomes S1: (9:16 - 9:17), S1: (9:17 - 9:18), S1: (9:18 - 9:19)
  private def toDiscreteSegment(segment: SegmentInfo, step: Duration): SegmentInfo = {
    val stepMillis = step.toMillis
    val truncatedStart = Instant.ofEpochMilli(segment.startTs - segment.startTs % stepMillis)
    val truncatedEnd = {
      val remainder = segment.endTs % stepMillis
      if (remainder == 0) {
        Instant.ofEpochMilli(segment.endTs)
      } else {
        Instant.ofEpochMilli(segment.endTs + stepMillis - remainder)
      }
    }
    segment.copy(startTs = truncatedStart.toEpochMilli, endTs = truncatedEnd.toEpochMilli)
  }

  // Group segments by the minute interval they represent, so for example:
  // S1: (9:16 - 9:19), S2: (9:17 - 9:19) becomes:
  // (9:16 - 9:17) -> S1
  // (9:17 - 9:18) -> S1, S2
  // (9:18 - 9:19) -> S1, S2
  // Note: that this also means a segment can get queried multiple times for a given query.
  private def computeGroups(
    segments: List[SegmentInfo],
    reverseSort: Boolean,
    queryStartTs: Long,
    queryEndTs: Long,
    minGroupSize: Int,
    step: Duration
  ): List[SegmentGroup] = {

    val discreteSegments = segments.map(seg => toDiscreteSegment(seg, step))

    val groupedByTime = discreteSegments
      .groupBy(seg => (seg.startTs, seg.endTs))
      .values
      .toList
      .map(segs => SegmentGroup(Math.max(segs.head.startTs, queryStartTs), segs.last.endTs, segs))
      .sortWith((s1, s2) => if (reverseSort) s1.endTs > s2.endTs else s1.endTs < s2.endTs)

    // Based on minGroupSize, merge segments into
    mergeContiguousGroups(
      groups = groupedByTime,
      queryStartTs = queryStartTs,
      queryEndTs = queryEndTs,
      minGroupSize = minGroupSize
    )
  }

  // Merge contiguous groups, so for example merge 9:16 - 9:17 and 9:17 - 9:18, which then becomes:
  // 9:16 - 9:18 -> (S1:(9:16 - 9:17), S1:(9:17 - 9:18), S2:(9:17 - 9:18)) = (S1:(9:16 - 9:18), S2:(9:17-9:18)
  private def mergeContiguousGroups(
    groups: List[SegmentGroup],
    queryStartTs: Long,
    queryEndTs: Long,
    minGroupSize: Int
  ): List[SegmentGroup] = {
    val mainBuilder = List.newBuilder[SegmentGroup]
    val tempBuilder = List.newBuilder[SegmentInfo]
    var numAdded = 0

    def addGroup(): Unit = {
      // Important note: We need to group by segmentId and baseExpr, otherwise segments for different data
      // expressions may get placed in two different groups.
      val groupedBySegmentIdBaseExpr = tempBuilder.result().groupBy(s => (s.segmentId, s.exprId))
      val newGroupSegments = groupedBySegmentIdBaseExpr.values.map(
        parts => parts.head.copy(startTs = parts.map(_.startTs).min, endTs = parts.map(_.endTs).max)
      )
      mainBuilder += SegmentGroup(
        startTs = Math.max(newGroupSegments.map(_.startTs).min, queryStartTs),
        endTs = Math.min(newGroupSegments.map(_.endTs).max, queryEndTs),
        segments = newGroupSegments.toList
      )
      tempBuilder.clear()
      numAdded = 0
    }

    for (group <- groups) {
      numAdded += group.segments.size
      tempBuilder ++= group.segments
      if (numAdded >= minGroupSize) {
        addGroup()
      }
    }
    if (numAdded > 0) addGroup()
    mainBuilder.result()
  }
}
