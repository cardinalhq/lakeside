package com.cardinal.discovery

import com.cardinal.utils.EnvUtils

trait ClusterScaler {
  def scaleTo(desiredReplicas: Int): Unit
}

object ClusterScaler {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)
  private val env = EnvUtils.mustGet("EXECUTION_ENVIRONMENT").toLowerCase

  def load(): ClusterScaler = env match {
    case "kubernetes" =>
      logger.info("Loading KubernetesScaler")
      val cfg = KubernetesClusterConfig.load()
      KubernetesScaler(cfg)

    case "ecs" =>
      logger.info("Loading EcsScaler")
      val cfg = EcsClusterConfig.load()
      EcsScaler(cfg)

    case "local" =>
      logger.info("Loading ConstantScaler for local execution")
      new ConstantScaler()

    case other =>
      throw new IllegalArgumentException(s"Unsupported EXECUTION_ENVIRONMENT: $other")
  }
}
