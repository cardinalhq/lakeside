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

import akka.NotUsed
import akka.stream.scaladsl.Source

object ConstantWatcher {
  /**
    * Returns a Source that emits one ClusterState (with Pod("127.0.0.1"))
    * and then never emits again.
    */
  def startWatching(): Source[ClusterState, NotUsed] = {
    val pod = Pod("127.0.0.1")

    val state = ClusterState(
      added = Set(pod),
      removed = Set.empty,
      current = Set(pod)
    )

    Source.single(state).concat(Source.never)
  }
}
