package com.cardinal.discovery

import net.jpountz.xxhash.XXHashFactory
import java.nio.charset.StandardCharsets

object Rendezvous {
  private val hasher = XXHashFactory
    .fastestJavaInstance()
    .hash64()

  def select[T](key: String, nodes: Seq[T], idFn: T => String): Option[T] = {
    if (nodes.isEmpty) None
    else {
      val seed = 0x9747b28cL // arbitrary seed value
      val (winner, _) = nodes.iterator
        .map { node =>
          val composite = key + "|" + idFn(node)
          val bytes     = composite.getBytes(StandardCharsets.UTF_8)
          val score     = hasher.hash(bytes, 0, bytes.length, seed)
          node -> score
        }
        .maxBy(_._2)
      Some(winner)
    }
  }
}
