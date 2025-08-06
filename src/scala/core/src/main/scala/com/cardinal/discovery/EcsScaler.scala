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

import software.amazon.awssdk.services.ecs.EcsClient
import software.amazon.awssdk.services.ecs.model.{UpdateServiceRequest, DescribeServicesRequest}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger

/**
 * A WorkerScaler backed by AWS ECS.
 *
 * @param clusterName   the ECS cluster name where your service is running
 * @param serviceName   the ECS service name to scale
 */
class EcsScaler(clusterName: String, serviceName: String) extends ClusterScaler {
  private val logger = LoggerFactory.getLogger(getClass)

  private val ecs: EcsClient = EcsClient.builder().build()
  private val lastDesired = new AtomicInteger(0)

  override def scaleTo(desiredReplicas: Int): Unit = {
    try {
      val req = UpdateServiceRequest.builder()
        .cluster(clusterName)
        .service(serviceName)
        .desiredCount(desiredReplicas)
        .build()

      ecs.updateService(req)
      logger.info(s"[ECS] scaleTo($desiredReplicas) requested for service '$serviceName' in cluster '$clusterName'")
      lastDesired.set(desiredReplicas)

    } catch {
      case e: Exception =>
        logger.error(s"[ECS] Error scaling service '$serviceName' to $desiredReplicas", e)
    }
  }
}

object EcsScaler {
  def apply(config: EcsClusterConfig): EcsScaler =
    new EcsScaler(config.clusterName, config.serviceName)
}
