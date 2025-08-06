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

import com.cardinal.utils.EnvUtils

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class EcsClusterConfig(
                      serviceName: String,
                      clusterName: String,
                      pollingInterval: FiniteDuration
                    )

object EcsClusterConfig {
  def load(): EcsClusterConfig = {
    val svc  = EnvUtils.mustFirstEnv(Seq("QUERY_WORKER_SERVICE_NAME", "ECS_WORKER_SERVICE_NAME"))
    val clst = EnvUtils.mustFirstEnv(Seq("QUERY_WORKER_CLUSTER_NAME", "ECS_WORKER_CLUSTER_NAME"))
    val intervalSecs = EnvUtils.get("WORKER_POLL_INTERVAL_SECONDS", default = "10").toInt
    EcsClusterConfig(svc, clst, intervalSecs.seconds)
  }
}
