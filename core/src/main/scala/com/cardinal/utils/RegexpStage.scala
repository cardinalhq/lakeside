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


import com.cardinal.utils.Commons.MESSAGE
import com.google.re2j.Pattern

import scala.collection.mutable

case class RegexpStage(regex: String, names: List[String] = List()) {
  private val re2Pattern = com.google.re2j.Pattern.compile(regex)
  private val tagNameExtractor = Pattern.compile("(<[a-zA-Z]+>)+").matcher(regex)
  private val tagNamesBuilder = List.newBuilder[String]
  if(names.nonEmpty) {
    tagNamesBuilder ++= names
  }
  else {
    while (tagNameExtractor.find()) {
      (1 to tagNameExtractor.groupCount()).foreach {
        tagIndex =>
          tagNamesBuilder += tagNameExtractor.group(tagIndex)
            .replace("<", "")
            .replace(">", "")
      }
    }
  }
  private val tagNames = tagNamesBuilder.result()

  def eval(tags: mutable.HashMap[String, Any]): Boolean = {
    val message = tags(MESSAGE).asInstanceOf[String]
    tags ++= eval(message)
    true
  }

  def eval(message: String): List[(String, String)] = {
    val re2Matcher = re2Pattern.matcher(message)
    val builder = List.newBuilder[(String, String)]
    if (re2Matcher.find()) {
      ((1 to re2Matcher.groupCount()) zip tagNames) foreach {
        case (index, tagName) => builder += tagName -> re2Matcher.group(index)
      }
    }
    builder.result()
  }
}



