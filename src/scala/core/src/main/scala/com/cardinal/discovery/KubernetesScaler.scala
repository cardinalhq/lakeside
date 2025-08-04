package com.cardinal.discovery

import io.fabric8.kubernetes.client.{ConfigBuilder, KubernetesClientBuilder, NamespacedKubernetesClient}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import java.util.concurrent.atomic.AtomicInteger

class KubernetesScaler(namespace: String, labels: Map[String, String]) extends ClusterScaler {
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
}

object KubernetesScaler {
  def apply(cfg: KubernetesClusterConfig): KubernetesScaler =
    new KubernetesScaler(cfg.namespace, cfg.serviceLabels)
}
