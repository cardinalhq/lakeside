package com.cardinal.discovery

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.NotUsed
import com.cardinal.utils.EnvUtils

object ClusterWatcher {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)
  private val env = EnvUtils.mustGet("EXECUTION_ENVIRONMENT")

  def watch()(implicit system: ActorSystem, mat: Materializer): Source[ClusterState, NotUsed] =
    env match {
      case "kubernetes" =>
        logger.info("Starting Kubernetes watcher")
        val cfg = KubernetesClusterConfig.load()
        KubernetesWatcher.startWatching(cfg.serviceLabels, cfg.namespace)

      case "ecs" =>
        logger.info("Starting ECS watcher")
        val cfg = EcsClusterConfig.load()
        EcsTaskWatcher.startWatching(cfg.serviceName, cfg.clusterName, cfg.pollingInterval)

      case "local" =>
        logger.info("Starting Constant watcher for local execution")
        ConstantWatcher.startWatching()

      case other =>
        throw new IllegalArgumentException(s"Unsupported EXECUTION_ENVIRONMENT: $other")
    }
}
