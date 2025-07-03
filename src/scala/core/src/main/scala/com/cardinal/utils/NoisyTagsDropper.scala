package com.cardinal.utils

import com.cardinal.utils.Commons.CARDINAL_HQ_PREFIX

import scala.collection.mutable

object NoisyTagsDropper {

  private val DO_NOT_DISPLAY_TAG_PREFIXES: Set[String] = Set("rollup_")
  private val DO_NOT_DISPLAY_TAGS: Set[String] = Set(
    "day",
    "month",
    "hour",
    "minute",
    "year",
    "sketch",
    s"$CARDINAL_HQ_PREFIX.tid",
    s"$CARDINAL_HQ_PREFIX.would_filter",
    s"$CARDINAL_HQ_PREFIX.trace_has_error",
    s"$CARDINAL_HQ_PREFIX.id",
    s"$CARDINAL_HQ_PREFIX.telemetry_type",
    s"$CARDINAL_HQ_PREFIX.filtered",
    s"$CARDINAL_HQ_PREFIX.is_root_span",
    s"$CARDINAL_HQ_PREFIX.positive_counts",
    s"$CARDINAL_HQ_PREFIX.negative_counts",
    "metric.stepTs",
    "metric.tagName",
    "metric.metrics_type",
    "scope.telemetry.sdk.name",
    "metric.filter",
    "metric.dd.israte",
    "metric.dd.rateinterval"
  )

  def remove(tags: mutable.HashMap[String, Any]): Unit = {
    val toRemove = tags.keySet.filter(
      t =>
        DO_NOT_DISPLAY_TAGS.contains(t) ||
        DO_NOT_DISPLAY_TAG_PREFIXES.exists(d => t.startsWith(d)) ||
        tags(t) == null ||
        tags(t).toString.isEmpty ||
        tags(t).toString == "null"
    )

    for (r <- toRemove) {
      tags.remove(r)
    }
  }
}
