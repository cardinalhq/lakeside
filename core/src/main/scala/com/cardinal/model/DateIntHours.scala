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

import com.cardinal.utils.Commons
import com.cardinal.utils.Commons.toDateIntFormat
import org.joda.time.DateTime

import scala.collection.mutable

case class DateIntHours(dateInt: String, hours: List[String])

object DateIntHours {
  def toDateHours(startDateTime: DateTime, endDateTime: DateTime): List[DateIntHours] = {
    var s = startDateTime
    var currentDateInt: String = ""
    val hours = Set.newBuilder[String]
    val q = new mutable.Queue[DateIntHours]()
    var lastDateHours: DateIntHours = null
    while (s.getMillis <= endDateTime.plusHours(1).getMillis) {
      val hour = s.getHourOfDay
      val dateIntStr = toDateIntFormat(s)
      val dateInt = dateIntStr
      if (currentDateInt.nonEmpty && dateInt != currentDateInt) {
        val newDateIntHours = DateIntHours(currentDateInt, hours = hours.result().toList.sorted)
        q += newDateIntHours
        lastDateHours = newDateIntHours
        hours.clear()
      }
      hours += Commons.toZeroFilledHour(hour)
      currentDateInt = dateInt
      s = s.plusHours(1)
    }
    val hoursResult = hours.result().toList.sorted
    if (currentDateInt.nonEmpty && hoursResult.nonEmpty) {
      val remaining = DateIntHours(currentDateInt, hours = hoursResult)
      q += remaining
    }
    q.toList
  }
}
