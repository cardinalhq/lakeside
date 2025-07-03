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
