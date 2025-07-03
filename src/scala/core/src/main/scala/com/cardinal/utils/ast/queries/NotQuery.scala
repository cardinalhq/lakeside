package com.cardinal.utils.ast.queries

import com.netflix.spectator.atlas.impl.Query

import java.util

case class NotQuery(q1: Query) extends Query {
  override def matches(tags: util.Map[String, String]): Boolean = {
    !q1.matches(tags)
  }

  override def dnfList: util.List[Query] = q1.dnfList()

}
