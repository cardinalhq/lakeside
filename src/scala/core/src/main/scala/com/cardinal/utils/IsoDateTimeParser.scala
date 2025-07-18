package com.cardinal.utils


import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}

/**
  * Helper for parsing the variations of ISO date/time formats that are used with Atlas. Since
  * the DateTimeFormatter doesn't have a way to check if a string matches, this class uses
  * pattern matching to normalize to a small number of cases (with and without zone) and avoid
  * using exceptions as the control flow.
  */
object IsoDateTimeParser {

  private val IsoDate = """^(\d{4}-\d{2}-\d{2})$""".r
  private val IsoDateZ = """^(\d{4}-\d{2}-\d{2})([-+Z].*)$""".r
  private val IsoDateTimeHHMM = """^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})$""".r
  private val IsoDateTimeHHMMZ = """^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})([-+Z].*)$""".r
  private val IsoDateTimeHHMMSS = """^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})$""".r
  private val IsoDateTimeHHMMSSZ = """^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})([-+Z].*)$""".r
  private val IsoDateTimeHHMMSSmmm = """^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3})$""".r

  private val IsoDateTimeHHMMSSmmmZ =
    """^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3})([-+Z].*)$""".r

  private val ZoneHour = """^([-+]\d{2})$""".r
  private val ZoneHourMinute = """^([-+]\d{2}):?(\d{2})$""".r
  private val ZoneHourMinuteSecond = """^([-+]\d{2})(\d{2})(\d{2})$""".r

  private val HasZone = """^.*([-+]\d{2}:\d{2}:\d{2}|Z)$""".r

  private def normalizeZone(zone: String): String = {
    zone match {
      case ZoneHour(h)                   => s"$h:00:00"
      case ZoneHourMinute(h, m)          => s"$h:$m:00"
      case ZoneHourMinuteSecond(h, m, s) => s"$h:$m:$s"
      case _                             => zone
    }
  }

  private def normalize(str: String): String = {
    str match {
      case IsoDate(d)                  => s"${d}T00:00:00"
      case IsoDateZ(d, z)              => s"${d}T00:00:00${normalizeZone(z)}"
      case IsoDateTimeHHMM(d)          => s"$d:00"
      case IsoDateTimeHHMMZ(d, z)      => s"$d:00${normalizeZone(z)}"
      case IsoDateTimeHHMMSS(d)        => s"$d"
      case IsoDateTimeHHMMSSZ(d, z)    => s"$d${normalizeZone(z)}"
      case IsoDateTimeHHMMSSmmm(d)     => s"$d"
      case IsoDateTimeHHMMSSmmmZ(d, z) => s"$d${normalizeZone(z)}"
      case _                           => str
    }
  }

  private def hasExplicitZone(str: String): Boolean = {
    str match {
      case HasZone(_) => true
      case _          => false
    }
  }

  def parse(str: String, tz: ZoneId): ZonedDateTime = {
    val timeStr = normalize(str)
    if (hasExplicitZone(timeStr))
      ZonedDateTime.parse(timeStr, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    else
      ZonedDateTime.parse(timeStr, DateTimeFormatter.ISO_DATE_TIME.withZone(tz))
  }
}
