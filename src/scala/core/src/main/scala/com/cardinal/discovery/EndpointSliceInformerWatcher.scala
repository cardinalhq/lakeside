package com.cardinal.discovery

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source}
import akka.stream.{KillSwitches, Materializer, OverflowStrategy}
import io.fabric8.kubernetes.api.model.discovery.v1.EndpointSlice
import io.fabric8.kubernetes.client.informers.{ResourceEventHandler, SharedInformerFactory}
import io.fabric8.kubernetes.client.{ConfigBuilder, KubernetesClient, KubernetesClientBuilder}
import org.slf4j.LoggerFactory

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

object EndpointSliceInformerWatcher {
  private val logger = LoggerFactory.getLogger(getClass)

  def startWatching(serviceName: String, namespace: String = "default")(implicit
                                                                        system: ActorSystem,
                                                                        mat: Materializer
  ): Source[ClusterState, NotUsed] = {
    val config = new ConfigBuilder()
      .withNamespace(namespace)
      .build()

    val client: KubernetesClient = new KubernetesClientBuilder().withConfig(config).build()
    val informerFactory: SharedInformerFactory = client.informers()
    val currentPods = new AtomicReference[Set[Pod]](Set.empty)

    val (queue, source) = Source
      .queue[ClusterState](64, OverflowStrategy.dropHead)
      .toMat(BroadcastHub.sink)(Keep.both)
      .run()

    def extractPods(slice: EndpointSlice): Set[Pod] = {
      Option(slice.getEndpoints).toSeq.flatMap(_.asScala).flatMap { ep =>
        val hostNameOpt = Option(ep.getHostname)
        val isLocal = hostNameOpt.contains(InetAddress.getLocalHost.getHostName)
        ep.getAddresses.asScala.map { ip =>
          val slotId = hostNameOpt.flatMap(toSlotIdFromHost).getOrElse(0)
          Pod(ip = ip, slotId = slotId, isLocal = isLocal)
        }
      }.toSet
    }

    def toSlotIdFromHost(hostname: String): Option[Int] = {
      val servicePrefix = s"$serviceName-"
      if (hostname.startsWith(servicePrefix)) {
        hostname.stripPrefix(servicePrefix).takeWhile(_.isDigit).toIntOption
      } else None
    }

    def rebuildPodSet(): Set[Pod] = {
      val store = informerFactory
        .getExistingSharedIndexInformer(classOf[EndpointSlice])
        .getStore

      store.list().asScala.flatMap {
        case es: EndpointSlice if hasServiceLabel(es, serviceName) =>
          extractPods(es)
        case _ => Nil
      }.toSet
    }

    def hasServiceLabel(es: EndpointSlice, svc: String): Boolean = {
      Option(es.getMetadata.getLabels)
        .flatMap(m => Option(m.get("kubernetes.io/service-name")))
        .contains(svc)
    }

    val handler = new ResourceEventHandler[EndpointSlice]() {
      override def onAdd(obj: EndpointSlice): Unit = onChange()
      override def onUpdate(oldObj: EndpointSlice, newObj: EndpointSlice): Unit = onChange()
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
          logger.info(s"[$serviceName] Cluster update: $state")
          queue.offer(state)
        }
      }
    }

    informerFactory
      .sharedIndexInformerFor(
        classOf[EndpointSlice],
        0 // resync period = 0 disables periodic forced resync
      )
      .addEventHandler(handler)

    informerFactory.startAllRegisteredInformers()

    source
  }
}

//object TestInformer extends App {
//  implicit val system: ActorSystem = ActorSystem("TestInformerSystem")
//  implicit val mat: Materializer = Materializer(system)
//  implicit val ec = system.dispatcher
//
//  // 1. Start the watcher and insert a KillSwitch
//  val (killSwitch, doneFuture) = EndpointSliceInformerWatcher
//    .startWatching("lakerunner-query-worker", "cardinalhq-datalake")
//    .viaMat(KillSwitches.single)(Keep.right)
//    .toMat(Sink.foreach { state: ClusterState =>
//      println(s"=== ClusterState @ ${java.time.Instant.now()} ===")
//      println(s"  Added:   ${state.added.map(_.ip).mkString(", ")}")
//      println(s"  Removed: ${state.removed.map(_.ip).mkString(", ")}")
//      println(s"  Current: ${state.current.map(_.ip).mkString(", ")}")
//      println()
//    })(Keep.both)
//    .run()
//
//  // 2. Schedule shutdown in 5 minutes
//  system.scheduler.scheduleOnce(5.minutes) {
//    println("⏳ Time’s up — shutting down stream and actor system.")
//    killSwitch.shutdown() // stops the stream
//    system.terminate() // tears down Akka
//  }
//}
