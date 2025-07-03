package com.cardinal.utils.ast.queries

import com.netflix.spectator.atlas.impl.Query.KeyQuery

case class InQuery(k: String, v: Set[String]) extends KeyQuery{
  override def key(): String = k

  override def matches(value: String): Boolean =
    value != null && v.contains(value)
}
