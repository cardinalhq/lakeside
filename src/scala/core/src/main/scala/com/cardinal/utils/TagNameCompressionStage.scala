package com.cardinal.utils

import scala.collection.mutable

case class TagNameCompressionStage() {

  private val seenTags = new mutable.HashSet[String]()

  def eval(tags: mutable.HashMap[String, Any]): Boolean = {
    val toRemove = tags.keySet.filter(t => !seenTags.add(t) || tags(t) == null || tags(t).toString.isEmpty)
    for (r <- toRemove) {
      tags.remove(r)
    }
    tags.nonEmpty
  }
}
