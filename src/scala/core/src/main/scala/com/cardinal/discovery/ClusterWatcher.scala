package com.cardinal.discovery

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.NotUsed
import com.cardinal.utils.EnvUtils

object ClusterWatcher {
  private val env =
    EnvUtils.mustGet("EXECUTION_ENVIRONMENT")

  def watch()(implicit system: ActorSystem, mat: Materializer): Source[ClusterState, NotUsed] =
    env match {
      case "kubernetes" =>
        val cfg = KubernetesClusterConfig.load()
        KubernetesWatcher.startWatching(cfg.serviceLabels, cfg.namespace)

      case "ecs" =>
        val cfg = EcsClusterConfig.load()
        EcsTaskWatcher.startWatching(cfg.serviceName, cfg.clusterName, cfg.pollingInterval)

      case "local" =>
        ConstantWatcher.startWatching()

      case other =>
        throw new IllegalArgumentException(s"Unsupported EXECUTION_ENVIRONMENT: $other")
    }
}
