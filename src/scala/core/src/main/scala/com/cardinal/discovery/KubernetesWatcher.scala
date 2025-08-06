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

import akka.NotUsed
import akka.stream.scaladsl.{BroadcastHub, Keep, Source}
import akka.stream.{Materializer, OverflowStrategy}
import com.cardinal.discovery.ClusterWatcher.getClass
import io.fabric8.kubernetes.api.model.Service
import io.fabric8.kubernetes.api.model.discovery.v1.{EndpointSlice, EndpointSliceList}
import io.fabric8.kubernetes.client.dsl.internal.OperationContext
import io.fabric8.kubernetes.client.informers.{ResourceEventHandler, SharedInformerFactory}
import io.fabric8.kubernetes.client.{ConfigBuilder, KubernetesClient, KubernetesClientBuilder, NamespacedKubernetesClient}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

object KubernetesWatcher {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Watch EndpointSlices for *any* Service whose labels match `serviceLabels`.
    *
    * @param serviceLabels   label-selector for Services, e.g. Map("app" → "lakerunner")
    * @param namespace       the Kubernetes namespace to watch
    */
  def startWatching(serviceLabels: Map[String, String], namespace: String)(
    implicit mat: Materializer
  ): Source[ClusterState, NotUsed] = {
    val logger = org.slf4j.LoggerFactory.getLogger(getClass)

    logger.info(s"Starting Kubernetes watcher for namespace: $namespace with labels: $serviceLabels")

    val config = new ConfigBuilder().withNamespace(namespace).build()
    val coreClient: KubernetesClient = new KubernetesClientBuilder().withConfig(config).build()
    val nsClient: NamespacedKubernetesClient = coreClient.adapt(classOf[NamespacedKubernetesClient]).inNamespace(namespace)
    val namespacedFactory: SharedInformerFactory = nsClient.informers().inNamespace(namespace)

    val matchingServices = new AtomicReference[Set[String]](Set.empty)

    def refreshServices(): Unit = {
      val svcNames = nsClient
        .services()
        .inNamespace(namespace)
        .withLabels(serviceLabels.asJava)
        .list()
        .getItems
        .asScala
        .map(_.getMetadata.getName)
        .toSet

      matchingServices.set(svcNames)
      logger.info(s"Matching Services: $svcNames")
    }

    refreshServices()

    nsClient.services()
      .inNamespace(namespace)
      .withLabels(serviceLabels.asJava)
      .inform(
        new ResourceEventHandler[Service] {
          override def onAdd(obj: Service): Unit = refreshServices()
          override def onUpdate(o: Service, n: Service): Unit = refreshServices()
          override def onDelete(obj: Service, b: Boolean): Unit = refreshServices()
        },
        0
      )

    val currentPods = new AtomicReference[Set[Pod]](Set.empty)
    val (queue, source) = Source
      .queue[ClusterState](64, OverflowStrategy.dropHead)
      .toMat(BroadcastHub.sink)(Keep.both)
      .run()

    def extractPods(slice: EndpointSlice): Set[Pod] = {
      Option(slice.getEndpoints).toSeq
        .flatMap(_.asScala)
        .flatMap { ep =>
          ep.getAddresses.asScala.map { ip =>
            Pod(ip)
          }
        }
        .toSet
    }

    def rebuildPodSet(): Set[Pod] = {
      val store = namespacedFactory
        .getExistingSharedIndexInformer(classOf[EndpointSlice])
        .getStore

      store
        .list()
        .asScala
        .flatMap {
          case es: EndpointSlice =>
            val labels = Option(es.getMetadata.getLabels).getOrElse(Map.empty.asJava)
            Option(labels.get("kubernetes.io/service-name"))
              .filter(matchingServices.get.contains)
              .toSeq
              .flatMap(_ => extractPods(es))

          case _ => Nil
        }
        .toSet
    }

    val handler = new ResourceEventHandler[EndpointSlice] {
      override def onAdd(obj: EndpointSlice): Unit = onChange()
      override def onUpdate(o: EndpointSlice, n: EndpointSlice): Unit = onChange()
      override def onDelete(obj: EndpointSlice, b: Boolean): Unit = onChange()

      private def onChange(): Unit = {
        val newSet = rebuildPodSet()
        val oldSet = currentPods.getAndSet(newSet)
        if (newSet != oldSet) {
          val state = ClusterState(
            added = newSet.diff(oldSet),
            removed = oldSet.diff(newSet),
            current = newSet
          )
          logger.info(s"Cluster update: $state")
          queue.offer(state)
        }
      }
    }

    val sliceInformer = namespacedFactory.sharedIndexInformerFor(classOf[EndpointSlice], 0)
    sliceInformer.addEventHandler(handler)

    namespacedFactory.startAllRegisteredInformers()

    source
  }
}
