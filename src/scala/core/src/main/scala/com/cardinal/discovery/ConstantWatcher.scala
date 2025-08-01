package com.cardinal.discovery

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import scala.concurrent.ExecutionContext

object ConstantWatcher {
  /**
   * Returns a Source that emits one ClusterState (with Pod("127.0.0.1", 0, true))
   * and then never emits again.
   */
  def startWatching(
                     serviceName: String,
                     namespace: String = "default"      // unused here, but keeps signature consistent
                   )(implicit
                     system: ActorSystem,
                     mat: Materializer
                   ): Source[ClusterState, NotUsed] = {
    implicit val ec: ExecutionContext = system.dispatcher

    val pod = Pod(ip = "127.0.0.1", slotId = 0)

    val state = ClusterState(
      added   = Set(pod),
      removed = Set.empty,
      current = Set(pod)
    )

    Source.single(state).concat(Source.never)
  }
}
