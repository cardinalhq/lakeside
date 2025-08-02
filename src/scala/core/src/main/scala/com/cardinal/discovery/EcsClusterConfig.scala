package com.cardinal.discovery

import com.cardinal.utils.EnvUtils

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class EcsClusterConfig(
                      serviceName: String,
                      clusterName: String,
                      pollingInterval: FiniteDuration
                    )

object EcsClusterConfig {
  def load(): EcsClusterConfig = {
    val svc  = EnvUtils.mustGet("ECS_WORKER_SERVICE_NAME")
    val clst = EnvUtils.mustGet("ECS_WORKER_CLUSTER_NAME")
    val intervalSecs = EnvUtils.get("WORKER_POLL_INTERVAL_SECONDS", default = "10").toInt
    EcsClusterConfig(svc, clst, intervalSecs.seconds)
  }
}
