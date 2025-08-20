/*
 * Copyright (C) 2025 CardinalHQ, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
