package com.cardinal.utils

import com.google.common.base.CaseFormat

import java.time._

/**
  * Helper functions for working with strings.
  */
object Strings {
  /**
    * Period following conventions of unix `at` command.
    */
  private val AtPeriod = """^(\d+)([a-z]+)$""".r

  /**
    * Period following the ISO8601 conventions.
    */
  private val IsoPeriod = """^(P.*)$""".r

  /**
    * Date relative to a given reference point.
    */
  private val RelativeDate = """^([a-z]+)([\-+])(.+)$""".r

  /**
    * Named date such as `epoch` or `now`.
    */
  private val NamedDate = """^([a-z]+)$""".r

  /**
    * Unix data in seconds since the epoch.
    */
  private val UnixDate = """^([0-9]+)$""".r

  /**
    * Returns true if a date string is relative. If custom ref is true it will
    * check if it is a relative date against a custom reference point other than
    * now or the epoch.
    */
  private def isRelativeDate(str: String, customRef: Boolean): Boolean = str match {
    case RelativeDate(r, _, _) => !customRef || (r != "now" && r != "epoch")
    case _ => false
  }

  /**
    * Return the time associated with a given string. The time will be relative
    * to `now`.
    */
  private def parseDate(str: String, tz: ZoneId = ZoneOffset.UTC): ZonedDateTime = {
    parseDate(ZonedDateTime.now(tz), str, tz)
  }

  /**
    * Return the time associated with a given string.
    *
    * - now, n:
    * - start, s:
    * - end, e:
    * - epoch:
    *
    * - seconds, s:
    * - minutes, m:
    * - hours, h:
    * - days, d:
    * - weeks, w:
    * - months
    * - years, y:
    */
  private def parseDate(ref: ZonedDateTime, str: String, tz: ZoneId): ZonedDateTime = str match {
    case RelativeDate(r, op, p) =>
      op match {
        case "-" => parseRefVar(ref, r).minus(parseDuration(p))
        case "+" => parseRefVar(ref, r).plus(parseDuration(p))
        case _ => throw new IllegalArgumentException("invalid operation " + op)
      }
    case NamedDate(r) =>
      parseRefVar(ref, r)
    case UnixDate(d) =>
      // If the value is too big assume it is a milliseconds unit like java uses. The overlap is
      // fairly small and not in the range we typically use:
      // scala> Instant.ofEpochMilli(Integer.MAX_VALUE)
      // res1: java.time.Instant = 1970-01-25T20:31:23.647Z
      val v = d.toLong
      val t = if (v > Integer.MAX_VALUE) v else v * 1000L
      ZonedDateTime.ofInstant(Instant.ofEpochMilli(t), tz)
    case str =>
      try IsoDateTimeParser.parse(str, tz)
      catch {
        case e: Exception => throw new IllegalArgumentException(s"invalid date $str", e)
      }
  }

  /**
    * Returns the datetime object associated with a given reference point.
    */
  private def parseRefVar(ref: ZonedDateTime, v: String): ZonedDateTime = {
    v match {
      case "now" => ZonedDateTime.now(ZoneOffset.UTC)
      case "epoch" => ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)
      case _ => ref
    }
  }

  /**
    * Parse a string that follows the ISO8601 spec or `at` time range spec
    * into a period object.
    */
  private def parseDuration(str: String): Duration = str match {
    case AtPeriod(a, u) => parseAtDuration(a, u)
    case IsoPeriod(_) => Duration.parse(str)
    case _ => throw new IllegalArgumentException("invalid period " + str)
  }

  /**
    * Convert an `at` command time range into a joda period object.
    */
  private def parseAtDuration(amount: String, unit: String): Duration = {
    val v = amount.toInt

    // format: off
    unit match {
      case "seconds" | "second" | "s" => Duration.ofSeconds(v)
      case "minutes" | "minute" | "min" | "m" => Duration.ofMinutes(v)
      case "hours" | "hour" | "h" => Duration.ofHours(v)
      case "days" | "day" | "d" => Duration.ofDays(v)
      case "weeks" | "week" | "wk" | "w" => Duration.ofDays(v * 7)
      case "months" | "month" => Duration.ofDays(v * 30)
      case "years" | "year" | "y" => Duration.ofDays(v * 365)
      case _ => throw new IllegalArgumentException("unknown unit " + unit)
    }
    // format: on
  }

  /**
    * Parse start and end time strings that can be relative to each other and resolve to
    * precise instants.
    *
    * @param s
    * Start time string in a format supported by `parseDate`.
    * @param e
    * End time string in a format supported by `parseDate`.
    * @param tz
    * Time zone to assume for the times if a zone is not explicitly specified. Defaults
    * to UTC.
    * @return
    * Tuple `start -> end`.
    */
  def timeRange(s: String, e: String, tz: ZoneId = ZoneOffset.UTC): (Instant, Instant) = {
    val range = if (Strings.isRelativeDate(s, customRef = true) || s == "e") {
      require(!Strings.isRelativeDate(e, customRef = true), "start and end are both relative")
      val end = Strings.parseDate(e, tz)
      val start = Strings.parseDate(end, s, tz)
      start.toInstant -> end.toInstant
    } else {
      val start = Strings.parseDate(s, tz)
      val end = Strings.parseDate(start, e, tz)
      start.toInstant -> end.toInstant
    }
    require(isBeforeOrEqual(range._1, range._2), "end time is before start time")
    range
  }

  private def isBeforeOrEqual(s: Instant, e: Instant): Boolean = s.isBefore(e) || s.equals(e)

  // Standardized date/time constants:
  private final val oneSecond = 1000L
  private final val oneMinute = oneSecond * 60L
  private final val oneHour = oneMinute * 60L
  private final val oneDay = oneHour * 24L
  private final val oneWeek = oneDay * 7L

  /**
    * Returns a string representation of a period.
    */
  def toString(d: Duration): String = {
    d.toMillis match {
      case t if t % oneWeek == 0 => s"${t / oneWeek}w"
      case t if t % oneDay == 0 => s"${t / oneDay}d"
      case t if t % oneHour == 0 => s"${t / oneHour}h"
      case t if t % oneMinute == 0 => s"${t / oneMinute}m"
      case t if t % oneSecond == 0 => s"${t / oneSecond}s"
      case _ => d.toString
    }
  }

  /**
    * Strip the margin from multi-line strings.
    */
  def stripMargin(str: String): String = {
    val s = str.stripMargin.trim
    s.replaceAll("\n\n+", "@@@").replaceAll("\n", " ").replaceAll("@@@", "\n\n")
  }

  def camelToSnakeCase(str: String): String = {
    val firstLowercaseIndex = str.indexWhere(c => c.isLower)
    if (firstLowercaseIndex > 0) {
      str.substring(0, firstLowercaseIndex).toLowerCase + CaseFormat.UPPER_CAMEL.to(
        CaseFormat.LOWER_UNDERSCORE,
        str.substring(firstLowercaseIndex)
      )
    } else {
      CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, str)
    }
  }
}
