package com.cardinal.utils.ast.queries

import com.netflix.spectator.atlas.impl.Query.KeyQuery

case class EqualsQuery(k: String, v: String) extends KeyQuery {
  override def key(): String = k

  override def matches(value: String): Boolean = {
    v == value
  }
}
