package com.cardinal.discovery

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{BroadcastHub, Keep, Source}
import akka.stream.{Materializer, OverflowStrategy}
import io.fabric8.kubernetes.api.model.Service
import io.fabric8.kubernetes.api.model.discovery.v1.EndpointSlice
import io.fabric8.kubernetes.client.informers.{ResourceEventHandler, SharedInformerFactory}
import io.fabric8.kubernetes.client.{ConfigBuilder, KubernetesClient, KubernetesClientBuilder}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

object KubernetesWatcher {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Watch EndpointSlices for *any* Service whose labels match `serviceLabels`.
   *
   * @param serviceLabels   label-selector for Services, e.g. Map("app" → "lakerunner")
   * @param namespace       the Kubernetes namespace to watch
   */
  def startWatching(
                     serviceLabels: Map[String, String],
                     namespace: String = "default"
                   )(implicit
                     system: ActorSystem,
                     mat: Materializer
                   ): Source[ClusterState, NotUsed] = {
    implicit val ec: ExecutionContext = system.dispatcher

    val config = new ConfigBuilder().withNamespace(namespace).build()
    val client: KubernetesClient =
      new KubernetesClientBuilder().withConfig(config).build()
    val informerFactory: SharedInformerFactory =
      client.informers()

    private val matchingServices = new AtomicReference[Set[String]](Set.empty)

    def refreshServices(): Unit = {
      val svcNames = client.services()
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

    informerFactory
      .sharedIndexInformerFor(classOf[Service], 0)
      .addEventHandler(new ResourceEventHandler[Service] {
        override def onAdd(obj: Service): Unit    = refreshServices()
        override def onUpdate(oldObj: Service, newObj: Service): Unit = refreshServices()
        override def onDelete(obj: Service, b: Boolean): Unit        = refreshServices()
      })

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
            val slotId = Option(ep.getHostname)
              .flatMap(toSlotIdFromHost)
              .getOrElse(0)
            Pod(ip = ip, slotId = slotId)
          }
        }
        .toSet
    }

    def toSlotIdFromHost(hostname: String): Option[Int] = {
      // you could make this pluggable too if you need
      hostname.split('-').lastOption.flatMap(_.toIntOption)
    }

    def rebuildPodSet(): Set[Pod] = {
      val store = informerFactory
        .getExistingSharedIndexInformer(classOf[EndpointSlice])
        .getStore

      store.list().asScala.flatMap {
        case es: EndpointSlice =>
          val labels = Option(es.getMetadata.getLabels).getOrElse(Map.empty.asJava)
          Option(labels.get("kubernetes.io/service-name"))
            .filter(matchingServices.get.contains)
            .toSeq
            .flatMap(_ => extractPods(es))

        case _ => Nil
      }.toSet
    }

    val handler = new ResourceEventHandler[EndpointSlice] {
      override def onAdd(obj: EndpointSlice): Unit    = onChange()
      override def onUpdate(o: EndpointSlice, n: EndpointSlice): Unit = onChange()
      override def onDelete(obj: EndpointSlice, b: Boolean): Unit     = onChange()

      private def onChange(): Unit = {
        val newSet = rebuildPodSet()
        val oldSet = currentPods.getAndSet(newSet)
        if (newSet != oldSet) {
          val state = ClusterState(
            added   = newSet.diff(oldSet),
            removed = oldSet.diff(newSet),
            current = newSet
          )
          logger.info(s"Cluster update: $state")
          queue.offer(state)
        }
      }
    }

    informerFactory
      .sharedIndexInformerFor(classOf[EndpointSlice], 0)
      .addEventHandler(handler)
    informerFactory.startAllRegisteredInformers()
    source
  }
}
