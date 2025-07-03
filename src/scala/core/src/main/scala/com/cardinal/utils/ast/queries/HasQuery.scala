package com.cardinal.utils.ast.queries

import com.netflix.spectator.atlas.impl.Query.KeyQuery

import java.util

case class HasQuery(k: String) extends KeyQuery {
  override def key(): String = k

  override def matches(value: String): Boolean = value != null

  override def matches(tags: util.Map[String, String]): Boolean = {
    tags.containsKey(k)
  }

}
