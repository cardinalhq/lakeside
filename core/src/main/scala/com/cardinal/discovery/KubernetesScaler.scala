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

import io.fabric8.kubernetes.client.{ConfigBuilder, KubernetesClientBuilder, NamespacedKubernetesClient}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import java.util.concurrent.atomic.AtomicInteger

class KubernetesScaler(namespace: String, labels: Map[String, String]) extends ClusterScaler with ScalingStateProvider {
  private val logger = LoggerFactory.getLogger(getClass)
  private val config = new ConfigBuilder().withNamespace(namespace).build()
  private val coreClient = new KubernetesClientBuilder().withConfig(config).build()
  private val nsClient = coreClient.adapt(classOf[NamespacedKubernetesClient]).inNamespace(namespace)
  private val lastDesired = new AtomicInteger(0)
  private val labelSelector = labels.asJava

  override def scaleTo(desiredReplicas: Int): Unit = {
    try {
      val deployments = nsClient
        .apps()
        .deployments()
        .inNamespace(namespace)
        .withLabels(labelSelector)
        .list()
        .getItems
        .asScala

      if (deployments.isEmpty) {
        logger.error(s"No deployments found in namespace='$namespace' with labels=$labels")
        return
      }

      deployments.foreach { d =>
        val name = d.getMetadata.getName
        nsClient
          .apps()
          .deployments()
          .inNamespace(namespace)
          .withName(name)
          .scale(desiredReplicas, true)
        logger.info(s"[$namespace/$name] scaleTo($desiredReplicas) requested")
      }
      lastDesired.set(desiredReplicas)
    } catch {
      case t: Throwable =>
        logger.error(s"Error scaling to $desiredReplicas replicas", t)
    }
  }

  override def getCurrentDesiredReplicas(): Int = {
    try {
      nsClient.apps().deployments()
        .inNamespace(namespace)
        .withLabels(labelSelector)
        .list().getItems.asScala
        .map(_.getSpec.getReplicas.intValue())
        .sum
    } catch {
      case _: Exception => 0
    }
  }

  override def getCurrentPodCount(): Int = {
    try {
      nsClient.pods()
        .inNamespace(namespace)
        .withLabels(labelSelector)
        .list().getItems.asScala
        .count(_.getStatus.getPhase == "Running")
    } catch {
      case _: Exception => 0
    }
  }
}

object KubernetesScaler {
  def apply(cfg: KubernetesClusterConfig): KubernetesScaler =
    new KubernetesScaler(cfg.namespace, cfg.serviceLabels)
}
