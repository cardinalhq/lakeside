package com.cardinal.utils.ast.queries

import com.netflix.spectator.atlas.impl.Query

import java.util

case class OrQuery(q1: Query, q2: Query) extends Query {
  override def matches(tags: util.Map[String, String]): Boolean = {
    q1.matches(tags) || q2.matches(tags)
  }

  override def not: Query = {
    val nq1 = q1.not
    val nq2 = q2.not
    nq1.and(nq2)
  }

  override def dnfList: util.List[Query] = {
    val qs = new util.ArrayList[Query](q1.dnfList)
    qs.addAll(q2.dnfList)
    qs
  }
}
