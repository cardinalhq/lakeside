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
import scala.jdk.CollectionConverters.ListHasAsScala

case class AndQuery(query1: Query, query2: Query) extends Query {
  override def matches(tags: util.Map[String, String]): Boolean = {
    query1.matches(tags) && query2.matches(tags)
  }

  override def not: Query = {
    val nq1 = query1.not
    val nq2 = query2.not
    nq1.or(nq2)
  }

  override def dnfList: util.List[Query] = crossAnd(query1.dnfList, query2.dnfList)

  override def andList: util.List[Query] = {
    val tmp = new util.ArrayList[Query](query1.andList)
    tmp.addAll(query2.andList)
    tmp
  }

  private def crossAnd(qs1: util.List[Query], qs2: util.List[Query]): util.ArrayList[Query] = {
    val tmp = new util.ArrayList[Query]
    for (q1 <- qs1.asScala) {
      for (q2 <- qs2.asScala) {
        tmp.add(q1.and(q2))
      }
    }
    tmp
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj) return true
    if (!obj.isInstanceOf[AndQuery]) return false
    val other: AndQuery = obj.asInstanceOf[AndQuery]
    this.query1 == other.query1 && this.query2 == other.query2
  }

  override def hashCode: Int = {
    var result = query1.hashCode
    result = 31 * result + query2.hashCode
    result
  }
}
