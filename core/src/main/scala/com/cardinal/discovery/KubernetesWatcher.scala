package com.cardinal.discovery

import akka.NotUsed
import akka.stream.scaladsl.{BroadcastHub, Keep, Source}
import akka.stream.{Materializer, OverflowStrategy}
import io.fabric8.kubernetes.api.model.{Pod => K8sPod}
import io.fabric8.kubernetes.client.informers.ResourceEventHandler
import io.fabric8.kubernetes.client.{ConfigBuilder, KubernetesClient, KubernetesClientBuilder, NamespacedKubernetesClient}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

object KubernetesWatcher {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Watch Pods in `namespace` whose labels include all entries in `podLabels`.
   * Emits ClusterState(added, removed, current) whenever the set of Ready pod IPs changes.
   *
   * Requirements:
   *  - ServiceAccount must have verbs: get, list, watch on "pods" in the namespace.
   *  - RBAC example:
   *      - Role:   resources: ["pods"], verbs: ["get","list","watch"]
   *      - Binding: subject = your SA (e.g., microbatch) in the same namespace
   */
  def startWatching(podLabels: Map[String, String], namespace: String)(
    implicit mat: Materializer
  ): Source[ClusterState, NotUsed] = {
    val log = LoggerFactory.getLogger(getClass)
    log.info(s"Starting Pod watcher for namespace='$namespace' with labels=$podLabels")

    // Build a namespaced client (scopes core operations to the namespace)
    val config = new ConfigBuilder().withNamespace(namespace).build()
    val coreClient: KubernetesClient =
      new KubernetesClientBuilder().withConfig(config).build()
    val nsClient: NamespacedKubernetesClient =
      coreClient.adapt(classOf[NamespacedKubernetesClient]).inNamespace(namespace)

    val current = new AtomicReference[Set[Pod]](Set.empty)

    val (queue, source) =
      Source.queue[ClusterState](64, OverflowStrategy.dropHead)
        .toMat(BroadcastHub.sink)(Keep.both)
        .run()

    // ---------- helpers ----------
    def isReady(p: K8sPod): Boolean = {
      val conds = Option(p.getStatus).flatMap(s => Option(s.getConditions)).map(_.asScala).getOrElse(Nil)
      conds.exists(c => "Ready" == c.getType && "True" == c.getStatus)
    }

    def podIp(p: K8sPod): Option[String] =
      Option(p.getStatus).flatMap(s => Option(s.getPodIP)).filter(_.nonEmpty)

    // Convert a Pod to our discovery model if it qualifies
    def asDiscovered(p: K8sPod): Option[Pod] =
      if (isReady(p)) podIp(p).map(Pod(_)) else None

    /** Atomically apply an update function to the current set and emit diffs if it changed. */
    def updateSet(f: Set[Pod] => Set[Pod]): Unit = {
      var done = false
      while (!done) {
        val oldSet = current.get()
        val newSet = f(oldSet)
        if (newSet ne oldSet) {
          if (current.compareAndSet(oldSet, newSet)) {
            if (newSet != oldSet) {
              val state = ClusterState(
                added  = newSet.diff(oldSet),
                removed = oldSet.diff(newSet),
                current = newSet
              )
              log.info(s"Cluster update: added=${state.added.size}, removed=${state.removed.size}, current=${state.current.size}")
              queue.offer(state)
            }
            done = true
          }
        } else {
          done = true
        }
      }
    }

    /** Remove any entry with the same IP, then (optionally) add the new one. */
    def upsert(p: K8sPod): Unit = {
      val candidate = asDiscovered(p)
      updateSet { s =>
        val withoutSameIp = candidate match {
          case Some(Pod(ip)) => s.filterNot(_.ip == ip)
          case None          => s
        }
        candidate match {
          case Some(pod) => withoutSameIp + pod
          case None      => withoutSameIp
        }
      }
    }

    /** Remove by IP if present (best effort on delete). */
    def remove(p: K8sPod): Unit = {
      val ipOpt = podIp(p)
      updateSet { s =>
        ipOpt match {
          case Some(ip) => s.filterNot(_.ip == ip)
          case None     => s // if no IP on delete, we leave the set unchanged
        }
      }
    }

    // ---------- informer (namespaced + server-side label selector) ----------
    val handler = new ResourceEventHandler[K8sPod] {
      override def onAdd(obj: K8sPod): Unit = upsert(obj)
      override def onUpdate(oldObj: K8sPod, newObj: K8sPod): Unit = upsert(newObj)
      override def onDelete(obj: K8sPod, deletedFinalStateUnknown: Boolean): Unit = remove(obj)
    }

    // Attach informer using server-side label selector; 0L disables periodic resync
      nsClient
        .pods()
        .withLabels(podLabels.asJava)
        .inform(handler, 0L)

    // Initial list happens inside the informer (it will emit ADDED for existing matching pods).
    // No further action needed here.

    source
  }
}