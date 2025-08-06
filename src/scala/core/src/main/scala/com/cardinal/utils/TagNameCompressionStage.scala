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

import scala.collection.mutable

case class TagNameCompressionStage() {

  private val seenTags = new mutable.HashSet[String]()

  def eval(tags: mutable.HashMap[String, Any]): Boolean = {
    val toRemove = tags.keySet.filter(t => !seenTags.add(t) || tags(t) == null || tags(t).toString.isEmpty)
    for (r <- toRemove) {
      tags.remove(r)
    }
    tags.nonEmpty
  }
}
