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

package com.cardinal.utils.ast.queries

import com.netflix.spectator.atlas.impl.Query

import java.util

case class NotQuery(q1: Query) extends Query {
  override def matches(tags: util.Map[String, String]): Boolean = {
    !q1.matches(tags)
  }

  override def dnfList: util.List[Query] = q1.dnfList()

}
