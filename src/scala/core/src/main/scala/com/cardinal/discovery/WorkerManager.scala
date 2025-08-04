package com.cardinal.discovery

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, HttpRequest}
import akka.http.scaladsl.unmarshalling.sse.EventStreamParser
import akka.pattern.after
import akka.stream.{Materializer, RestartSettings}
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import com.cardinal.utils.Commons.{QUERY_WORKER_HEARTBEAT, QUERY_WORKER_PORT}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration.{Deadline, Duration, DurationInt, MILLISECONDS}
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

  private def startHeartBeatingWithQueryWorker(pod: Pod)
                                              (implicit system: ActorSystem, mat: Materializer): Future[Done] = {
    val ip       = pod.ip
    val port     = QUERY_WORKER_PORT
    val uri      = s"http://$ip:$port$QUERY_WORKER_HEARTBEAT"
    val request  = HttpRequest(uri = uri)
    @volatile var added = false
    val deadline = Deadline.now + 1.minute

    def attemptOnce(): Future[Unit] =
      Http().singleRequest(request).flatMap { resp =>
        resp.entity.dataBytes
          .via(EventStreamParser(Int.MaxValue, Int.MaxValue))
          .filter(_.data.nonEmpty)
          .take(1)
          .runWith(Sink.head)
          .map { _ =>
            if (!added) {
              readyPods.updateAndGet(_ + pod)
              added = true
              logger.info(s"Heartbeat established for $ip")
            }
          }
      }

    def retryInitial(): Future[Unit] =
      attemptOnce().recoverWith {
        case _ if deadline.hasTimeLeft =>
          after(2.seconds, system.scheduler)(retryInitial())
      }

    def steadyStream(): Future[Done] =
      Http().singleRequest(request).flatMap { resp =>
        resp.entity.dataBytes
          .via(EventStreamParser(Int.MaxValue, Int.MaxValue))
          .map(_ => ())
          .runWith(Sink.ignore)
      }

    val flow: Future[Done] = retryInitial().flatMap(_ => steadyStream())

    flow.onComplete { _ =>
      readyPods.updateAndGet(_ - pod)
      logger.info(s"Heartbeat terminated for $ip, removed from readyPods")
      // TODO: ask your manager to murder this pod now that it really is gone
    }

    flow
  }
}

object WorkerManager {
  def apply()(implicit system: ActorSystem, mat: Materializer): WorkerManager = {
    val minWorkers = sys.env.getOrElse("NUM_MIN_QUERY_WORKERS", "2").toInt
    val maxWorkers = sys.env.getOrElse("NUM_MAX_QUERY_WORKERS", "30").toInt
    new WorkerManager(ClusterWatcher.watch(), minWorkers, maxWorkers, ClusterScaler.load())
  }
}
