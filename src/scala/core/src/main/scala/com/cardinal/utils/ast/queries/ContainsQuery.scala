package com.cardinal.utils.ast.queries

import com.netflix.spectator.atlas.impl.Query.KeyQuery

case class ContainsQuery(k: String, contained: String) extends KeyQuery {

  override def key(): String = k

  override def matches(value: String): Boolean = {
    value != null && value.contains(contained)
  }
}
