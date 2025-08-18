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

package com.cardinal.queryapi.engine

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.cardinal.datastructures.EMA
import com.cardinal.discovery.{ClusterScaler, ClusterWatcher, Pod, WorkerManager}
import com.cardinal.model.query.common.SegmentInfo
import com.cardinal.model.{DownloadSegmentRequest, Heartbeat, ScalingStatusMessage, SegmentRequest}
import com.cardinal.queryapi.engine.SegmentCacheManager.{getWorkerFor, manager, readyPodCount, timeOfLastQuery, toSegmentPathOnS3}
import com.cardinal.utils.Commons._
import com.cardinal.utils.StreamUtils
import com.netflix.atlas.json.Json
import io.kubernetes.client.openapi.Configuration
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContextExecutor

object SegmentCacheManager {
  implicit val as: ActorSystem = ActorSystem("SegmentCacheSystem")
  implicit val mat: Materializer = Materializer(as)
  implicit val ec: ExecutionContextExecutor = as.dispatcher

  private val metadataLookupTimes = new AtomicReference[EMA](new EMA(0.7))
  private val totalQueryTimes = new AtomicReference[EMA](new EMA(0.7))
  private val timeOfLastQuery = new AtomicLong(0)
  private val client = io.kubernetes.client.util.Config.defaultClient
  Configuration.setDefaultApiClient(client)

  private val heartbeatReceiver = new WorkerHeartbeatReceiver()
  private val manager = {
    val minWorkers = sys.env.getOrElse("NUM_MIN_QUERY_WORKERS", "2").toInt
    val maxWorkers = sys.env.getOrElse("NUM_MAX_QUERY_WORKERS", "30").toInt
    new WorkerManager(
      ClusterWatcher.watch(),
      minWorkers,
      maxWorkers,
      ClusterScaler.load(),
      () => heartbeatReceiver.getReadyWorkerCount,
      heartbeatReceiver.getWorkerFor
    )
  }

  def waitUntilScaled(): Source[ScalingStatusMessage, NotUsed] = {
    manager.recordQuery()
    manager.waitForSufficientWorkers()
  }

  def getWorkerFor(segmentId: String): Option[Pod] = manager.getWorkerFor(segmentId)

  def readyPodCount: Int = manager.podCount

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
}

class SegmentCacheManager()(implicit actorSystem: ActorSystem) {
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  implicit val mat: akka.stream.Materializer = akka.stream.Materializer(actorSystem)
  private val logger = LoggerFactory.getLogger(getClass)
  private val CACHE_URI = "/api/internal/cacheSegments"
  locally {
    SegmentCacheManager.manager
  }

  private val _downloadQueue = StreamUtils
    .blockingQueue[Seq[SegmentInfo]]("downloadQueue", 1024)
    .flatMapConcat { segments =>
      val byPod: Map[Pod, Seq[SegmentInfo]] =
        segments
          .flatMap(s => getWorkerFor(s.segmentId).map(p => p -> s))
          .groupBy(_._1)
          .view
          .mapValues(_.map(_._2))
          .toMap

      Source(byPod.toList)
    }
    .wireTap(e => logger.info(s"Requesting ${e._1.ip} to download ${e._2.size} segments"))
    .mapAsync(PARALLELISM) {
      case (pod, segments) =>
        val batch = segments.map { seg =>
          DownloadSegmentRequest(
            bucketName = seg.bucketName,
            toSegmentPathOnS3(
              bucketName = seg.bucketName,
              dataset = seg.dataset,
              dateInt = seg.dateInt,
              hour = seg.hour,
              segmentId = seg.segmentId,
              customerId = seg.customerId,
              collectorId = seg.collectorId
            )
          )
        }.toList

        Http()
          .singleRequest(
            HttpRequest(
              method = HttpMethods.POST,
              uri = s"http://${pod.ip}:$SERVICE_PORT$CACHE_URI",
              entity = HttpEntity(Json.encode(batch))
            )
          )
          .recover {
            case ex =>
              logger.error(s"Error POSTing to ${pod.ip}", ex)
              throw ex
          }
          .map(_.entity.discardBytes())
    }
    .toMat(Sink.ignore)(Keep.left)
    .run()

  def readyPods: Int = readyPodCount

  def enqueueCacheRequest(segments: Seq[SegmentInfo]): Unit = {
    timeOfLastQuery.set(System.currentTimeMillis())
    val sealedOnly = segments.filter(_.sealedStatus)
    if (sealedOnly.nonEmpty) _downloadQueue.offer(sealedOnly)
  }

  def getGroupedByQueryWorkerPod(segmentRequests: List[SegmentRequest]): Map[Pod, List[SegmentRequest]] = {
    if (readyPodCount == 0) Map.empty
    else
      segmentRequests
        .flatMap { req =>
          getWorkerFor(req.segmentId).map(pod => pod -> req)
        }
        .groupMap(_._1)(_._2)
  }
}
