package com.cardinal.discovery

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, HttpRequest}
import akka.http.scaladsl.unmarshalling.sse.EventStreamParser
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.cardinal.utils.Commons.{QUERY_WORKER_HEARTBEAT, QUERY_WORKER_PORT}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration.{Duration, DurationInt, MILLISECONDS}
import scala.util.{Failure, Success}

class WorkerManager(
  watcher: Source[ClusterState, NotUsed],
  minWorkers: Int,
  maxWorkers: Int,
  scaler: ClusterScaler
)(implicit system: ActorSystem, mat: Materializer) {
  private implicit val ec: ExecutionContextExecutor = system.dispatcher

  private final val logger = LoggerFactory.getLogger(getClass)

  private val currentPods = new AtomicReference[Set[Pod]](Set.empty)
  private val readyPods = new AtomicReference[Set[Pod]](Set.empty)
  private val timeOfLastQuery = new AtomicLong(0)
  private val timeOfLastScaleRequest = new AtomicLong(0)
  private val cfg = ConfigFactory.load()
  private val scaleUpWaitTime = cfg.getInt("scale-up.wait.time.minutes")
  private val scaleDownWaitTime = cfg.getInt("scale-down.wait.time.minutes")

  watcher.runForeach { state =>
    currentPods.set(state.current)

    state.added.foreach { pod =>
      startHeartBeatingWithQueryWorker(pod).onComplete {
        case Success(_) =>
          readyPods.updateAndGet(_ + pod)
        case Failure(ex) =>
          system.log.warning(s"Health-check failed for $pod: $ex")
      }
    }

    state.removed.foreach { pod =>
      readyPods.updateAndGet(_ - pod)
    }

    enforceBoundsAndMaybeScaleDown()
  }

  system.scheduler.scheduleWithFixedDelay(
    initialDelay = 1.minute,
    delay        = 1.minute
  )(() =>
    logger.info(
      s"Known ready pods: ${readyPods.get().map(_.ip).mkString("[", ", ", "]")}"
    )
  )(ec)


  system.scheduler.scheduleWithFixedDelay(
    initialDelay = scaleDownWaitTime.minutes,
    delay = scaleDownWaitTime.minutes
  )(() => enforceBoundsAndMaybeScaleDown())

  private def enforceBoundsAndMaybeScaleDown(): Unit = {
    val sz = currentPods.get.size

    if (sz > maxWorkers) {
      scaleTo(maxWorkers)
      return
    }

    val idleMinutes = minutesSince(timeOfLastQuery)
    val sinceLastScale = minutesSince(timeOfLastScaleRequest)
    if (idleMinutes >= scaleDownWaitTime &&
        sinceLastScale >= scaleDownWaitTime &&
        sz > minWorkers) {
      scaleTo(minWorkers)
    }
  }

  def isScaledUp: Boolean = {
    val sz = podCount
    sz >= maxWorkers
  }

  def recordQuery(): Unit = {
    timeOfLastQuery.set(System.currentTimeMillis())
    maybeScaleUp()
  }

  private def maybeScaleUp(): Unit = {
    val sinceLast = minutesSince(timeOfLastScaleRequest)
    if (timeOfLastScaleRequest.get == 0 || sinceLast >= scaleUpWaitTime) {
      scaleTo(maxWorkers)
    }
  }

  private def scaleTo(desired: Int): Unit = {
    val tgt = math.max(minWorkers, math.min(maxWorkers, desired))
    scaler.scaleTo(tgt)
    timeOfLastScaleRequest.set(System.currentTimeMillis())
  }

  private def minutesSince(al: AtomicLong): Long = {
    val ts = al.get()
    if (ts == 0) 0
    else Duration(System.currentTimeMillis() - ts, MILLISECONDS).toMinutes
  }

  def podCount: Int = readyPods.get().size

  def getWorkerFor(key: String): Option[Pod] = {
    val pods = readyPods.get().toSeq
    Rendezvous.select[Pod](key, pods, _.ip)
  }

  private def startHeartBeatingWithQueryWorker(pod: Pod)(implicit system: ActorSystem): Future[Done] = {
    val ip = pod.ip

    logger.info(s"Starting to heartbeat with ${pod.ip}")
    var alreadyAdded = false

    Source
      .single(HttpRequest(uri = QUERY_WORKER_HEARTBEAT, entity = HttpEntity("")))
      .via(Http().outgoingConnection(host = ip, port = QUERY_WORKER_PORT))
      .flatMapConcat(resp => resp.entity.dataBytes)
      .via(EventStreamParser(Int.MaxValue, Int.MaxValue))
      .map { _ =>
        if (!alreadyAdded) {
          readyPods.updateAndGet(_ + pod)
          alreadyAdded = true
          logger.info(s"Heartbeat established for $ip, added to readyPods")
        }
      }
      .watchTermination() { (_, f) =>
        f.onComplete { _ =>
          readyPods.updateAndGet(_ - pod)
          logger.info(s"Heartbeat terminated for $ip, removed from readyPods")
        }
      }
      .toMat(Sink.ignore)(Keep.right)
      .run()
  }
}

object WorkerManager {
  def apply()(implicit system: ActorSystem, mat: Materializer): WorkerManager = {
    val minWorkers = sys.env.getOrElse("NUM_MIN_QUERY_WORKERS", "2").toInt
    val maxWorkers = sys.env.getOrElse("NUM_MAX_QUERY_WORKERS", "30").toInt
    new WorkerManager(ClusterWatcher.watch(), minWorkers, maxWorkers, ClusterScaler.load())
  }
}
