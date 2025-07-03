package com.cardinal.utils.ast.queries

import com.netflix.spectator.atlas.impl.Query.KeyQuery
import com.netflix.spectator.impl.PatternMatcher

case class RegexQuery(k: String, regex: String) extends KeyQuery {
  private val pattern = PatternMatcher.compile("^" + regex)

  override def key(): String = k

  override def matches(value: String): Boolean = {
    value != null && pattern.matches(value)
  }
}
