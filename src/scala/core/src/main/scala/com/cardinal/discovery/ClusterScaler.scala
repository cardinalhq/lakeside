package com.cardinal.discovery

import com.cardinal.utils.EnvUtils

trait ClusterScaler {
  def scaleTo(desiredReplicas: Int): Unit
}

object ClusterScaler {
  private val env = EnvUtils.mustGet("EXECUTION_ENVIRONMENT").toLowerCase

  def load(): ClusterScaler = env match {
    case "kubernetes" =>
      val cfg = KubernetesClusterConfig.load()
      KubernetesScaler(cfg)

    case "ecs" =>
      val cfg = EcsClusterConfig.load()
      EcsScaler(cfg)

    case "local" =>
      new ConstantScaler()

    case other =>
      throw new IllegalArgumentException(s"Unsupported EXECUTION_ENVIRONMENT: $other")
  }
}
