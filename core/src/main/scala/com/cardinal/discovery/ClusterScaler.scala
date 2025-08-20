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
