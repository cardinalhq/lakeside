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

import com.cardinal.logs.LogCommons.{DATA_SIZE_TYPE, DURATION_TYPE}

import java.util.regex.Pattern

object QuantityParser {
  private val quantityRegex: Pattern = Pattern.compile("([0-9]+(.[0-9]+)?)(\\w+|µs)")
  private val minuteToNanos: Double => Double = (num: Double) => num * 60 * 1000000000L
  private val secondsToNanos: Double => Double = (num: Double) => num * 1000000000L
  private val millisToNanos: Double => Double = (num: Double) => num * 1000000L
  private val microsToNanos: Double => Double = (num: Double) => num * 1000L
  private val hourToNanos: Double => Double = (num: Double) => num * 3600 * 1000000000L
  private val dayToNanos: Double => Double = (num: Double) => num * 24 * 3600 * 1000000000L
  private val nanosToNanos: Double => Double = (num: Double) => num
  private val bytesToBytes: Double => Double = (num: Double) => num
  private val kiloByteToBytes: Double => Double = (num: Double) => num * 1000
  private val megaByteToBytes: Double => Double = (num: Double) => num * 1000000
  private val gigaByteToBytes: Double => Double = (num: Double) => num * 1000000000
  private val teraByteToBytes: Double => Double = (num: Double) => num * 1000000000000L
  private val petaByteToBytes: Double => Double = (num: Double) => num * 1000000000000000L
  private val mibToBytes: Double => Double = (num: Double) => num * 131072
  private val kibToBytes: Double => Double = (num: Double) => num * 128
  private val gibToBytes: Double => Double = (num: Double) => num * 134200000L
  private val tibToBytes: Double => Double = (num: Double) => num * 137400000000L
  private val pibToBytes: Double => Double = (num: Double) => num * 1126000000000000L

  private val durationNormalizations: Map[String, Double => Double] = Map[String, Double => Double](
    "s" -> secondsToNanos,
    "sec" -> secondsToNanos,
    "secs" -> secondsToNanos,
    "second" -> secondsToNanos,
    "seconds" -> secondsToNanos,
    "m" -> minuteToNanos,
    "min" -> minuteToNanos,
    "mins" -> minuteToNanos,
    "minute" -> minuteToNanos,
    "minutes" -> minuteToNanos,
    "ms" -> millisToNanos,
    "milli" -> millisToNanos,
    "millis" -> millisToNanos,
    "millisecond" -> millisToNanos,
    "milliseconds" -> millisToNanos,
    "µs" -> microsToNanos,
    "micro" -> microsToNanos,
    "micros" -> microsToNanos,
    "microsecond" -> microsToNanos,
    "microseconds" -> microsToNanos,
    "ns" -> nanosToNanos,
    "h" -> hourToNanos,
    "hr" -> hourToNanos,
    "hrs" -> hourToNanos,
    "hour" -> hourToNanos,
    "hours" -> hourToNanos,
    "d" -> dayToNanos,
    "day" -> dayToNanos,
    "days" -> dayToNanos
  )

  private val sizeNormalizations: Map[String, Double => Double] = Map[String, Double => Double](
    "b" -> bytesToBytes,
    "byte" -> bytesToBytes,
    "bytes" -> bytesToBytes,
    "k" -> kiloByteToBytes,
    "kb" -> kiloByteToBytes,
    "kilobyte" -> kiloByteToBytes,
    "kilobytes" -> kiloByteToBytes,
    "m" -> megaByteToBytes,
    "mb" -> megaByteToBytes,
    "mbs" -> megaByteToBytes,
    "megabyte" -> megaByteToBytes,
    "g" -> gigaByteToBytes,
    "gb" -> gigaByteToBytes,
    "gbs" -> gigaByteToBytes,
    "gigabyte" -> gigaByteToBytes,
    "gigabytes" -> gigaByteToBytes,
    "t" -> teraByteToBytes,
    "tb" -> teraByteToBytes,
    "tbs" -> teraByteToBytes,
    "terabyte" -> teraByteToBytes,
    "terabytes" -> teraByteToBytes,
    "pb" -> petaByteToBytes,
    "pbs" -> petaByteToBytes,
    "petabyte" -> petaByteToBytes,
    "petabytes" -> petaByteToBytes,
    "mib" -> mibToBytes,
    "mibs" -> mibToBytes,
    "mebibyte" -> mibToBytes,
    "mebibytes" -> mibToBytes,
    "kib" -> kibToBytes,
    "kibs" -> kibToBytes,
    "kibibyte" -> kibToBytes,
    "kibibytes" -> kibToBytes,
    "gib" -> gibToBytes,
    "gibs" -> gibToBytes,
    "gibibyte" -> gibToBytes,
    "gibibytes" -> gibToBytes,
    "tib" -> tibToBytes,
    "tibs" -> tibToBytes,
    "tibibyte" -> tibToBytes,
    "tibibytes" -> tibToBytes,
    "pib" -> pibToBytes,
    "pibs" -> pibToBytes,
    "pibibyte" -> pibToBytes,
    "pibibytes" -> pibToBytes
  )

  private def parseQuantityUnit(value: Any): Option[(String, String)] = {
    val matcher = quantityRegex.matcher(value.toString)
    if (matcher.find()) {
      val quantity = matcher.group(1)
      val unit = matcher.group(3)
      Some(quantity, unit.toLowerCase)
    } else None
  }

  def parseQuantity(value: Any, `type`: String): Option[Double] = {
    parseQuantityUnit(value).flatMap {
      case (quantity, unit) =>
        `type` match {
          case DATA_SIZE_TYPE => sizeNormalizations.get(unit).map(func => func.apply(quantity.toDouble))
          case DURATION_TYPE => durationNormalizations.get(unit).map(func => func.apply(quantity.toDouble))
        }
    }
  }
}
