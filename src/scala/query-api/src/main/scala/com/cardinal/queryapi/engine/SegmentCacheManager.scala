package com.cardinal.queryapi.engine

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.cardinal.datastructures.EMA
import com.cardinal.discovery.DiscoveryService.getTargetPod
import com.cardinal.discovery.{DiscoveryService, Pod}
import com.cardinal.model.query.common.SegmentInfo
import com.cardinal.model.{DownloadSegmentRequest, Heartbeat, SegmentRequest}
import com.cardinal.queryapi.engine.SegmentCacheManager.{timeOfLastQuery, toSegmentPathOnS3}
import com.cardinal.utils.Commons._
import com.cardinal.utils.StreamUtils
import com.netflix.atlas.json.Json
import io.kubernetes.client.openapi.apis.AppsV1Api
import io.kubernetes.client.openapi.{ApiException, Configuration}
import org.slf4j.LoggerFactory

import java.net.InetAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

object SegmentCacheManager {
  private val QUERY_SLA = 5000
  private val logger = LoggerFactory.getLogger(getClass)

  private val metadataLookupTimes = new AtomicReference[EMA](new EMA(0.7))
  private val totalQueryTimes = new AtomicReference[EMA](new EMA(0.7))
  private val timeOfLastQuery = new AtomicLong(0)
  private val timeOfLastScaleRequest = new AtomicLong(0)
  private val scaleUpWaitTime = config.getInt("scale-up.wait.time.minutes")
  private val scaleDownWaitTime = config.getInt("scale-down.wait.time.minutes")
  // Initialize kubernetes java client..
  private val client = io.kubernetes.client.util.Config.defaultClient
  Configuration.setDefaultApiClient(client)

  def waitUntilScaled(queryId: String): Source[Heartbeat, NotUsed] = {
    Source
      .tick(0.seconds, 3.seconds, NotUsed)
      .takeWhile(_ => DiscoveryService.getNumPods(QUERY_WORKER) < getMaxQueryWorkers)
      .wireTap(_ => scaleIfPossible(queryId))
      .map { _ =>
        Heartbeat(`type` = "waiting_scale_up")
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  def startDiscovery()(implicit as: ActorSystem): Unit = {
    if (isRunningInKubernetes && !isGlobalQueryStack) {
      DiscoveryService(QUERY_WORKER).runForeach { clusterState =>
        val numQueryWorkers = clusterState.current.size
        val slotId = DiscoveryService.toSlotId(s"query-api", InetAddress.getLocalHost.getHostName)
        if (slotId == 0) {
          logger.info(s"Number of query workers = ${DiscoveryService.getNumPods(QUERY_WORKER)}")
          val minutesSinceLastQuery = minutesSince(timeOfLastQuery)
          val minPodAge = TimeUnit.MINUTES
            .convert(System.currentTimeMillis() - DiscoveryService.getYoungestWorkerStartTime, TimeUnit.MILLISECONDS)
          val offTimeQueryCapacity = sys.env("NUM_MIN_QUERY_WORKERS").toInt
          if (minutesSinceLastQuery >= scaleDownWaitTime && minPodAge >= scaleDownWaitTime && numQueryWorkers > offTimeQueryCapacity) {
            logger.info(
              s"No queries in the last $scaleDownWaitTime minutes, scaling workers down to $offTimeQueryCapacity," +
              s" minutesSinceLastQuery = $minutesSinceLastQuery"
            )
            scaleQueryWorkers("default", offTimeQueryCapacity)
          }
        }
      }
    }
  }

  def toSegmentPathOnS3(
    bucketName: String,
    dataset: String,
    dateInt: String,
    hour: String,
    segmentId: String,
    customerId: String,
    collectorId: String
  ): String = {
    s"${getDbPath(bucketName = bucketName, customerId = customerId, collectorId = collectorId, dataset, dateInt, hour)
      .replace("./db", "db")}/$segmentId.parquet"
  }

  def updateTotalQueryTime(time: Long): Double = {
    totalQueryTimes.get().set(time.toDouble)
    totalQueryTimes.get().value.getOrElse(0.0)
  }

  def updateMetadataLookupTime(time: Long): Unit = {
    metadataLookupTimes.get().set(time.toDouble)
  }

  private def getScaleTo: Int = {
    val maxCapacity = getMaxQueryWorkers
    val current = DiscoveryService.getNumPods(QUERY_WORKER)
    val totalTime = totalQueryTimes.get().value.getOrElse(0.0)
    val metadataLookupTime = metadataLookupTimes.get().value.getOrElse(0.0)
    val queryWorkerTime = totalTime - metadataLookupTime
    if (queryWorkerTime >= QUERY_SLA) {
      Math.min(maxCapacity, current + Math.min(maxCapacity - current, 10))
    } else -1
  }

  private def getMaxQueryWorkers = {
    sys.env.getOrElse("NUM_MAX_QUERY_WORKERS", "30").toInt
  }

  private def scaleQueryWorkers(queryId: String, replicaCount: Int): Unit = {
    try {
      // Create an API instance
      val api = new AppsV1Api()
      // Define deployment details
      val namespace = sys.env.getOrElse("POD_NAMESPACE", "cardinalhq")
      val deploymentName = QUERY_WORKER

      // Get the existing deployment
      val deployment = api.readNamespacedDeployment(deploymentName, namespace, null)

      // Update the replica count
      val spec = deployment.getSpec
      if (spec != null) {
        spec.setReplicas(replicaCount)
        // Update the deployment with the new replica count
        api.replaceNamespacedDeployment(deploymentName, namespace, deployment, null, null, null, null)
        logger.info(s"[$queryId] Successfully requested worker scale to $replicaCount")
        timeOfLastScaleRequest.set(System.currentTimeMillis())
      } else {
        logger.error(s"[$queryId] Could not find $QUERY_WORKER deployment!")
      }
    } catch {
      case e: ApiException =>
        if (e.getCode == 409) {
          timeOfLastScaleRequest.set(System.currentTimeMillis())
        }
        logger.error(s"Error in scaling query workers ${e.getMessage} ${e.getResponseBody}", e)
    }
  }

  private def scaleIfPossible(queryId: String): Unit = {
    val minutesSinceLastScaleRequest = minutesSince(timeOfLastScaleRequest)
    val numToGet = Math.max(getScaleTo, getMaxQueryWorkers)
    if (timeOfLastScaleRequest.get() == 0 || minutesSinceLastScaleRequest >= scaleUpWaitTime) {
      if (!isGlobalQueryStack) {
        scaleQueryWorkers(queryId, numToGet)
      }
    }
  }

  private def minutesSince(al: AtomicLong): Long = {
    if (al.get() == 0) {
      0
    } else {
      TimeUnit.MINUTES.convert(System.currentTimeMillis() - al.get(), TimeUnit.MILLISECONDS)
    }
  }
}

class SegmentCacheManager(actorSystem: ActorSystem) {
  implicit val as: ActorSystem = actorSystem
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  private val logger = LoggerFactory.getLogger(getClass)
  private val CACHE_URI = "/api/internal/cacheSegments"

  private val _downloadQueue = StreamUtils
    .blockingQueue[Seq[SegmentInfo]](id = "downloadQueue", 1024)
    .filter(_ => DiscoveryService.getNumPods(QUERY_WORKER) > 0)
    .flatMapConcat(
      segments => Source(segments.groupBy(s => getTargetPod(QUERY_WORKER, s.segmentId)))
    )
    .wireTap(e => logger.info(s"Requesting ${e._1.ip} to download ${e._2.size} segments"))
    .mapAsync(PARALLELISM) { groupedByPod =>
      Future {
        val (pod, segments) = groupedByPod
        val batch = segments
          .map(
            segment =>
              DownloadSegmentRequest(
                bucketName = segment.bucketName,
                toSegmentPathOnS3(
                  bucketName = segment.bucketName,
                  dataset = segment.dataset,
                  dateInt = segment.dateInt,
                  hour = segment.hour,
                  segmentId = segment.segmentId,
                  customerId = segment.customerId,
                  collectorId = segment.collectorId
                )
            )
          )
          .toList
        Http()
          .singleRequest(
            HttpRequest(
              method = HttpMethods.POST,
              entity = HttpEntity(Json.encode[List[DownloadSegmentRequest]](batch)),
              uri = s"http://${pod.ip}:$SERVICE_PORT$CACHE_URI"
            )
          )
          .recover {
            case e: Exception =>
              logger.error("Error in request", e)
              throw new RuntimeException(e)
          }
          .map(response => response.entity.discardBytes())
      }
    }
    .toMat(Sink.ignore)(Keep.left)
    .run()

  def getGroupedByQueryWorkerPod(segmentRequests: List[SegmentRequest]): Map[Pod, List[SegmentRequest]] = {
    if (DiscoveryService.getNumPods(QUERY_WORKER) == 0) Map.empty
    else segmentRequests.groupBy(s => getTargetPod(QUERY_WORKER, s.segmentId))
  }

  def enqueueCacheRequest(segments: Seq[SegmentInfo]): Unit = {
    timeOfLastQuery.set(System.currentTimeMillis())
    val sealedSegments = segments.filter(_.sealedStatus)
    if (sealedSegments.nonEmpty) {
      _downloadQueue.offer(sealedSegments)
    }
  }
}
