package com.cardinal.discovery

import akka.actor.ActorSystem
import akka.discovery.{Discovery, Lookup, ServiceDiscovery}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, HttpRequest}
import akka.http.scaladsl.unmarshalling.sse.EventStreamParser
import akka.stream.scaladsl.{BroadcastHub, Keep, RestartSource, Sink, Source}
import akka.stream.{RestartSettings, ThrottleMode}
import akka.{Done, NotUsed}
import com.cardinal.model.SlotInfo
import com.cardinal.utils.Commons
import com.cardinal.utils.Commons._
import com.cardinal.utils.transport.HashRing
import org.slf4j.LoggerFactory

import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

object DiscoveryService {
  private final val logger = LoggerFactory.getLogger(getClass)
  private val sourcesMap = new ConcurrentHashMap[String, Source[ClusterState, NotUsed]]()
  private val slotInfos = new ConcurrentHashMap[String, SlotInfo]()
  private val hashRings = new ConcurrentHashMap[String, HashRing]()
  private val queryWorkerServiceName = sys.env.getOrElse("QUERY_WORKER_SERVICE_NAME",
    throw new RuntimeException("QUERY_WORKER_SERVICE_NAME environment variable is not set!")
  )
  private val heartBeatingQueryWorkers = new ConcurrentHashMap[String, Long]() // podIp -> heartbeat websocket source

  def getYoungestWorkerStartTime: Long = {
    if(heartBeatingQueryWorkers.isEmpty) return 0L
    val minWorkerAge = heartBeatingQueryWorkers.values().asScala.max
    logger.info(s"Min worker age = $minWorkerAge")
    minWorkerAge
  }

  def getTargetPod(segmentId: String): Pod = {
    val infoRef = slotInfos.get(queryWorkerServiceName)
    if (infoRef.numSlots == 0) {
      logger.error(s"Could not find target pod for s$queryWorkerServiceName/$segmentId")
      null
    } else {
      val slotId = Math.abs(segmentId.replace("tbl_", "").hashCode) % infoRef.numSlots
      // This can happen when the cluster is restarting
      if (!infoRef.podBySlot.contains(slotId)) {
        logger.warn(s"Did not find slotId = $slotId, falling back to local")
        infoRef.podBySlot.values.find(_.isLocal).get
      } else infoRef.podBySlot(slotId)
    }
  }

  def getNumPods: Int = {
    val slotInfo = slotInfos.get(queryWorkerServiceName)
    if (slotInfo == null) 0
    else slotInfo.numSlots
  }

  private def toAddresses(resolved: ServiceDiscovery.Resolved): Set[Pod] = {
    resolved.addresses
      .map(resolved => {
        if (!Commons.isRunningInKubernetes) {
          Pod(ip = resolved.host, slotId = 0, isLocal = true)
        } else {
          val ip = resolved.address.get.getHostAddress
          val slotId = toSlotId(resolved.address.get.getHostName)
          val localHostName = InetAddress.getLocalHost.getCanonicalHostName
          val localSlotId = toSlotId(localHostName)
          Pod(ip = ip, slotId = slotId, isLocal = slotId == localSlotId)
        }
      })
      .toSet
  }

  def toSlotId(hostName: String = InetAddress.getLocalHost.getHostName): Int = {
    Try(hostName.split("\\.").head.replace(s"$queryWorkerServiceName-", "").toInt).getOrElse(-1)
  }

  private def lookup(serviceDiscovery: ServiceDiscovery): Source[ServiceDiscovery.Resolved, NotUsed] = {
    val eventualResolved = serviceDiscovery.lookup(lookup = Lookup(queryWorkerServiceName), 30.seconds)
    Source.future(eventualResolved)
  }

  private def getHashRing: HashRing = {
    hashRings.computeIfAbsent(queryWorkerServiceName, (_: String) => new HashRing)
  }

  private def updateQueryWorkerSlotInfo(pod: Pod, shouldAdd: Boolean): Unit = {
    if (shouldAdd) {
      slotInfos.computeIfAbsent(queryWorkerServiceName, _ => SlotInfo(0, Map.empty))
    }
    slotInfos.computeIfPresent(
      queryWorkerServiceName,
      (_, slotInfo) => {
        val newPods = if (shouldAdd) {
          slotInfo.podBySlot.values ++ List(pod)
        } else {
          slotInfo.podBySlot.values.filter(!_.equals(pod))
        }
        val newPodsBySlot = newPods.toList
          .distinctBy(pod => pod.ip)
          .sortBy(_.ip)
          .zipWithIndex
          .map(e => e._2 -> e._1.copy(slotId = e._2))
          .toMap
        SlotInfo(numSlots = newPods.size, podBySlot = newPodsBySlot)
      }
    )
    if (shouldAdd) {
      logger.info(s"Successfully added ${pod.ip} to $queryWorkerServiceName slotInfos")
    } else {
      logger.info(s"Removed ${pod.ip} from $queryWorkerServiceName slotInfos")
    }
  }

  private def startHeartBeatingWithQueryWorker(queryWorkerPod: Pod)(implicit as: ActorSystem): Future[Done] = {
    implicit val es: ExecutionContextExecutor = as.getDispatcher
    // Only add the query-worker to slotInfos when it establishes the heartbeat websocket connection
    val ip = queryWorkerPod.ip
    logger.info(s"Starting to heartbeat with ${queryWorkerPod.ip}")
    val alreadyAdded = new AtomicBoolean(false)

    Source
      .single(HttpRequest(uri = QUERY_WORKER_HEARTBEAT, entity = HttpEntity("")))
      .via(Http().outgoingConnection(host = ip, port = QUERY_WORKER_PORT))
      .flatMapConcat(resp => resp.entity.dataBytes)
      .via(EventStreamParser(Int.MaxValue, Int.MaxValue))
      .map { _ =>
        if (!alreadyAdded.get()) {
          updateQueryWorkerSlotInfo(queryWorkerPod, shouldAdd = true)
          alreadyAdded.set(true)
        }
      }
      .watchTermination() { (_, f) =>
        f.onComplete { _ =>
          // if the heartbeat fails, remove the pod from the query-worker slotInfo
          updateQueryWorkerSlotInfo(queryWorkerPod, shouldAdd = false)
          heartBeatingQueryWorkers.remove(queryWorkerPod.ip)
        }
      }
      .toMat(Sink.ignore)(Keep.right)
      .run()
  }

  def apply(serviceName: String = queryWorkerServiceName)(implicit as: ActorSystem): Source[ClusterState, NotUsed] = {
    sourcesMap.computeIfAbsent(
      serviceName,
      (_: String) => {
        val serviceDiscovery = if (isRunningInKubernetes) {
          Discovery(as).loadServiceDiscovery("kubernetes-api")
        } else {
          Discovery(as).loadServiceDiscovery("config")
        }

        logger.info(s"Using service name: $serviceName")

        RestartSource
          .withBackoff(RestartSettings(5.seconds, 1.minute, 0.3)) {
            () =>
              Source
                .repeat(NotUsed)
                .throttle(1, 30.seconds, 1, ThrottleMode.Shaping)
                .flatMapConcat(_ => lookup(serviceDiscovery))
                .map(resolved => toAddresses(resolved))
                .statefulMapConcat {
                  () =>
                    var current = Set[Pod]()
                    resolved =>
                      {
                        val isNotStatefulSet = resolved.forall(_.slotId == -1)
                        var modifiedResolved = if (isNotStatefulSet) {
                          resolved.toList
                            .sortWith((p1, p2) => p1.lastTwoIpSegments < p2.lastTwoIpSegments)
                            .zipWithIndex
                            .map(e => e._1.copy(slotId = e._2, isLocal = false))
                            .toSet
                        } else resolved

                        modifiedResolved = modifiedResolved
                          .map(p => if (p.slotId == -1) p.copy(slotId = 0) else p.copy(slotId = p.slotId))

                        val `new` = modifiedResolved
                        val added = `new` -- current
                        val removed = current -- `new`
                        if (added.nonEmpty) logger.info(s"[$serviceName] Adding pod(s) ${added.mkString(", ")}")
                        if (removed.nonEmpty) logger.info(s"[$serviceName] Removing pod(s) ${removed.mkString(", ")}")
                        current = `new`
                        Some(ClusterState(added = added, removed = removed, current = current))
                      }
                }
          }
          .alsoTo(Sink.foreach {
            clusterState =>
              {
                if (serviceName.equals(queryWorkerServiceName)) {
                  clusterState.current.foreach(pod => {
                    heartBeatingQueryWorkers.computeIfAbsent(pod.ip, _ => {
                      startHeartBeatingWithQueryWorker(pod)
                      System.currentTimeMillis()
                    })
                  })
                } else {
                  slotInfos.put(
                    serviceName,
                    SlotInfo(
                      numSlots = clusterState.current.size,
                      podBySlot = clusterState.current.map(pod => pod.slotId -> pod).toMap
                    )
                  )

                  val hashRing = getHashRing
                  clusterState.added.foreach { addedPod =>
                    hashRing.add(addedPod)
                  }

                  clusterState.removed.foreach { removedPod =>
                    hashRing.remove(removedPod)
                  }
                }
              }
          })
          .toMat(BroadcastHub.sink[ClusterState](16))(Keep.right)
          .run()
      }
    )
  }
}
