package com.cardinal.queryworker

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpEntity, MediaTypes, StatusCodes}
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.ThrottleMode
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.cardinal.model.{DataPoint, DownloadSegmentRequest, GenericSSEStringPayload, PushDownRequest}
import com.cardinal.objectstorage.ObjectStore
import com.cardinal.utils.Commons._
import com.cardinal.utils.StreamUtils
import com.cardinal.utils.ast.SketchInput
import com.github.benmanes.caffeine.cache.{Caffeine, RemovalCause}
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.json.Json
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.DependsOn
import org.springframework.stereotype.Component

import java.io.File
import java.nio.file.Files
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

@Component
@DependsOn(Array("storageProfileCache"))
class WorkerApi @Autowired()(actorSystem: ActorSystem, objectStore: ObjectStore) extends WebApi with Directives {
  implicit val as: ActorSystem = actorSystem
  implicit val ec: ExecutionContext = actorSystem.dispatcher
  private val os: ObjectStore = objectStore
  private val dbFile = new File("./db")
  dbFile.mkdirs()

  private val logger = LoggerFactory.getLogger(getClass)
  private val cache = Caffeine
    .newBuilder()
    .weigher((_: DownloadSegmentRequest, value: Integer) => value)
    .executor(as.dispatcher)
    .maximumWeight(9000000000L) // allow 9GB to be stored locally, beyond that evict.
    .evictionListener((key: DownloadSegmentRequest, _: Integer, cause: RemovalCause) => {
      val deleted = Files.deleteIfExists(new File(s"./$key").toPath)
      if (deleted) {
        logger.info(s"Deleted file $key from diskCache, cause = ${cause.name()}")
      }
    })
    .build[DownloadSegmentRequest, Integer]()

  private val downloadQueue = StreamUtils
    .blockingQueue[DownloadSegmentRequest]("downloadQueue", 1024)
    .filter(path => cache.getIfPresent(path) == null) // only download what hasn't been downloaded before
    .throttle(1, 1.second, 1, ThrottleMode.Shaping) // slow down the download rate to 1 per second
    .map { dsr =>
      val (downloaded, size) = os.downloadObject(bucketName = dsr.bucketName, dsr.key)
      if (downloaded) {
        cache.put(dsr, Int.box(size.toInt))
      }
    }
    .toMat(Sink.ignore)(Keep.left)
    .run()

  private def cacheSegments: Route = path("api" / "internal" / "cacheSegments") {
    post {
      entity(as[String]) { payload =>
        val paths = Json.decode[List[DownloadSegmentRequest]](payload).filter(dsr => cache.getIfPresent(dsr) == null) // only download what hasn't been downloaded before
        if (paths.nonEmpty) {
          paths.foreach { path =>
            logger.info(s"Received cache request for ${paths.size} segments..")
            downloadQueue.offer(path)
          }
        }
        complete(StatusCodes.OK) // OK to always return `OK` since this is a best effort operation.
      }
    }
  }

  private def healthCheck: Route = {
    path("ready") {
      complete {
        StatusCodes.OK
      }
    }
  }
  private def streamDataRoute(
    queryId: String,
    localParquet: Boolean,
    pushDownRequest: PushDownRequest
  )(implicit as: ActorSystem, ec: ExecutionContext): Source[Either[DataPoint, SketchInput], NotUsed] = {
    Source
      .single(pushDownRequest)
      .flatMapConcat(
        pushDownRequest => evaluatePushDownRequest(queryId, localParquet, pushDownRequest)
      )
      .recover {
        case e: Exception =>
          logger.error(
            s"[$queryId] Error in streamDataRoute for segments: ${pushDownRequest.segmentRequests.map(s => s.segmentId).mkString(", ")}",
            e
          )
          throw new RuntimeException(e)
      }
  }

  private def streamCachedSegment: Route = {
    path("api" / "internal" / "timeseries") {
      parameter("queryId".?) { queryId =>
        post {
          entity(as[String]) { payload =>
            {
              val pushDownRequest = PushDownRequest.fromJson(payload)
              implicit val ord: Ordering[Either[DataPoint, SketchInput]] = pushDownResponseOrdering(pushDownRequest)

              val requests = pushDownRequest.segmentRequests
              val (locallyCached, sealedOnS3) = requests.partition(
                e => {
                  val segmentPath = toSegmentPathOnS3(
                    bucketName = e.bucketName,
                    dataset = e.dataset,
                    dateInt = e.dateInt,
                    hour = e.hour,
                    segmentId = e.segmentId,
                    customerId = e.customerId,
                    collectorId = e.collectorId
                  )
                  cache
                    .getIfPresent(
                      DownloadSegmentRequest(bucketName = e.bucketName, key = segmentPath)
                    ) != null
                }
              )

              logger.info(
                s"[${queryId.getOrElse("")}] Received request: sealed = ${sealedOnS3.size}, cached = ${locallyCached.size}"
              )

              val lcSource = if (locallyCached.nonEmpty) {
                streamDataRoute(
                  queryId = queryId.getOrElse(""),
                  localParquet = true,
                  pushDownRequest.copy(segmentRequests = locallyCached)
                )
              } else Source.empty

              val sealedSource = if (sealedOnS3.nonEmpty) {
                streamDataRoute(
                  queryId = queryId.getOrElse(""),
                  localParquet = false,
                  pushDownRequest.copy(segmentRequests = sealedOnS3)
                )
              } else Source.empty

              complete {
                HttpEntity.Chunked(
                  MediaTypes.`text/event-stream`.toContentType,
                  dataPointResponseToSSE(
                    List(lcSource, sealedSource).fold(Source.empty)((s1, s2) => s1.mergeSorted(s2))
                  )
                )
              }
            }
          }
        }
      }
    }
  }

  private def heartbeat: Route = {
    path("api" / "internal" / "heartbeat") {
      complete {
        HttpEntity.Chunked(
          MediaTypes.`text/event-stream`.toContentType,
          Source
            .tick(1.seconds, 5.seconds, NotUsed)
            .map { _ =>
              GenericSSEStringPayload(message = "heartbeat").toChunkStreamPart
            }
        )
      }
    }
  }

  override def routes: Route = {
    cacheSegments ~ streamCachedSegment ~ healthCheck ~ heartbeat
  }
}
