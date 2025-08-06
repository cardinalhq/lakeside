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

package com.cardinal.instrumentation

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.{DoubleCounter, Meter, ObservableDoubleMeasurement}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

object Metrics {
  // Build and install the SDK meter provider as the global
  private lazy val meter: Meter = GlobalOpenTelemetry.getMeter("com.cardinal.instrumentation")

  private val histograms = new ConcurrentHashMap[String, io.opentelemetry.api.metrics.DoubleHistogram]()
  private val counters = new ConcurrentHashMap[String, DoubleCounter]()
  private val PREFIX = sys.env.getOrElse("METRICS_PREFIX", "default")
  private val gaugeValues = new ConcurrentHashMap[String, AtomicReference[Double]]()
  private val registeredGauges = new ConcurrentHashMap[String, Boolean]()

  private def metricNameWithPrefix(name: String): String =
    s"$PREFIX.$name"

  private def buildAttributes(tags: Seq[String]): Attributes = {
    val b = Attributes.builder()
    tags.foreach { t =>
      val Array(k, v) = t.split(":", 2)
      b.put(k, v)
    }
    b.build()
  }

  def recordExecutionTime(metricName: String, durationMillis: Long, tags: String*): Unit = {
    val hist = histograms.computeIfAbsent(
      metricNameWithPrefix(metricName),
      name =>
        meter
          .histogramBuilder(name)
          .setUnit("ms")
          .setDescription("Execution time in ms")
          .build()
    )
    hist.record(durationMillis.toDouble, buildAttributes(tags))
  }

  def count(metricName: String, delta: Double, tags: String*): Unit = {
    val c = counters.computeIfAbsent(
      metricNameWithPrefix(metricName),
      name =>
        meter
          .counterBuilder(name)
          .setUnit("1")
          .setDescription("Manual counter")
          .ofDoubles()
          .build()
    )
    c.add(delta, buildAttributes(tags))
  }

  def gauge(metricName: String, value: Double, tags: String*): Unit = {
    val attributes = buildAttributes(tags)
    val key = metricNameWithPrefix(metricName) + attributes.toString

    val ref = gaugeValues.computeIfAbsent(key, _ => new AtomicReference[Double](value))
    ref.set(value)

    registeredGauges.computeIfAbsent(
      key,
      _ => {
        meter
          .gaugeBuilder(metricNameWithPrefix(metricName))
          .setUnit("1")
          .setDescription("Manual gauge")
          .buildWithCallback((measurement: ObservableDoubleMeasurement) => {
            Option(gaugeValues.get(key)).foreach { a =>
              measurement.record(a.get(), attributes)
            }
          })
        true
      }
    )
  }
}
