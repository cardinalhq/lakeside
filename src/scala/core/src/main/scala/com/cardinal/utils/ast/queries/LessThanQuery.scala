package com.cardinal.utils.ast.queries

import com.netflix.spectator.atlas.impl.Query.KeyQuery


case class LessThanQuery(k: String, v: Any) extends KeyQuery {
  override def key(): String = k

  override def matches(value: String): Boolean = {
    v match {
      case vd: Double => value.toDouble < vd
      case _ => false
    }
  }
}
