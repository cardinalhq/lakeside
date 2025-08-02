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
