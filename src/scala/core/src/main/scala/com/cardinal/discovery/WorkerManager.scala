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

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, HttpRequest}
import akka.http.scaladsl.unmarshalling.sse.EventStreamParser
import akka.pattern.after
import akka.stream.{Materializer, RestartSettings}
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import com.cardinal.model.ScalingStatusMessage
import com.cardinal.utils.Commons._
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
  scaler: ClusterScaler,
  getHeartbeatingWorkers: () => Int,
  getHeartbeatingWorkerFor: String => Option[Pod],
  isLegacyMode: Boolean = false
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

  private val scalingMetrics = scaler match {
    case s: ClusterScaler with ScalingStateProvider => Some(new ScalingMetrics(s, getHeartbeatingWorkers))
    case _ => None
  }

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
    val sz = getHeartbeatingWorkers()
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
    getHeartbeatingWorkerFor(key)
  }

  def waitForSufficientWorkers(): Source[ScalingStatusMessage, NotUsed] = {
    val readyWorkers = getHeartbeatingWorkers()
    
    // If using legacy mode, don't wait for heartbeating workers
    if (isLegacyMode) {
      logger.warn("Using legacy WorkerManager implementation - skipping worker wait")
      return Source.single(ScalingStatusMessage("Skipping worker wait - using legacy implementation"))
    }
    
    val minWorkersAbsolute = math.min(MIN_WORKERS_FOR_QUERY, maxWorkers)
    val desiredWorkers = scalingMetrics.map(_.getDesiredWorkers).getOrElse(maxWorkers)
    val minWorkersPercent = math.ceil(desiredWorkers * MIN_WORKERS_PERCENT / 100.0).toInt
    val effectiveMinWorkers = math.max(1, math.max(minWorkersAbsolute, minWorkersPercent))

    // If we already have enough workers, return immediately
    if (readyWorkers >= effectiveMinWorkers) {
      return Source.single(ScalingStatusMessage(s"Ready: $readyWorkers workers available"))
    }

    Source.tick(1.second, 1.second, NotUsed)
      .scan(0)((acc, _) => acc + 1)
      .takeWhile { secondsWaited =>
        val currentWorkers = getHeartbeatingWorkers()
        val shouldContinueWaiting = currentWorkers < effectiveMinWorkers &&
                                   secondsWaited < MAX_WORKER_WAIT_SECONDS

        if (secondsWaited % 5 == 0) {
          logger.info(s"Waiting for workers: ${currentWorkers}/${effectiveMinWorkers} ready, ${secondsWaited}s elapsed")
        }

        shouldContinueWaiting
      }
      .map { secondsWaited =>
        val currentWorkers = getHeartbeatingWorkers()
        if (currentWorkers >= effectiveMinWorkers) {
          ScalingStatusMessage(s"Ready: $currentWorkers workers available")
        } else {
          ScalingStatusMessage(s"Proceeding with $currentWorkers workers after ${MAX_WORKER_WAIT_SECONDS}s timeout")
        }
      }
      .take(1)
      .mapMaterializedValue(_ => NotUsed)
  }

  private def startHeartBeatingWithQueryWorker(pod: Pod)
                                              (implicit system: ActorSystem, mat: Materializer): Future[Done] = {
    val ip       = pod.ip
    val port     = QUERY_WORKER_PORT
    val uri      = s"http://$ip:$port$QUERY_WORKER_HEARTBEAT"
    val request  = HttpRequest(uri = uri)
    @volatile var added = false
    val deadline = Deadline.now + 1.minute

    logger.info(s"Discovered new query worker at $ip, attempting heartbeat")

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
        case _ if deadline.hasTimeLeft() =>
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
    new WorkerManager(
      ClusterWatcher.watch(),
      minWorkers,
      maxWorkers,
      ClusterScaler.load(),
      () => 0, // Default implementation
      _ => None, // Default implementation
      isLegacyMode = true // Mark as legacy mode
    )
  }
}
