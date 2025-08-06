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

package com.cardinal.logs

object LogCommons {

  val EXISTS: String = "exists"
  val NOT_EQUALS: String = "!="
  val REGEX: String = "regex"
  val IN: String = "in"
  val NOT_IN: String = "not_in"
  val CONTAINS: String = "contains"
  val STRING_TYPE: String = "string"
  val NUMBER_TYPE: String = "number"
  val DURATION_TYPE: String = "duration"
  val DATA_SIZE_TYPE: String = "datasize"

  // ASL operators
  val EQ = "eq"
  val HAS = "has"
  val GT = "gt"
  val GE = "ge"
  val LT = "lt"
  val LE = "le"

  val MAX = "max"
  val MIN = "min"
  val SUM = "sum"
  val COUNT = "count"
  val AVG = "avg"

}
