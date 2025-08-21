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

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.unmarshalling.sse.EventStreamParser
import akka.pattern.after
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import com.cardinal.utils.Commons._
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.collection.mutable
import scala.concurrent.duration.{Deadline, Duration, DurationInt, MILLISECONDS}
import scala.concurrent.{ExecutionContextExecutor, Future}

class WorkerManager(watcher: Source[ClusterState, NotUsed],
                    minWorkers: Int,
                    maxWorkers: Int,
                    scaler: ClusterScaler,
                    isLegacyMode: Boolean = false
                   )(implicit system: ActorSystem, mat: Materializer) {

  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  private val logger = LoggerFactory.getLogger(getClass)

  private val currentPods = new AtomicReference[Set[Pod]](Set.empty)
  private val readyPods = new AtomicReference[Set[Pod]](Set.empty)
  private val podsBySlotId = new AtomicReference[Map[Int, Pod]]()
  private val lastQueryTs = new AtomicLong(0)
  private val lastScaleTs = new AtomicLong(0)

  private val scaleUpWaitMinutes = sys.env.getOrElse("SCALE_UP_WAIT_MINUTES", "1").toInt
  private val scaleDownWaitMinutes = sys.env.getOrElse("SCALE_DOWN_WAIT_MINUTES", "10").toInt

  // Provide a truthful heartbeat supplier for metrics
  private val heartbeatsFn: () => Int = () => readyPods.get().size

  private val scalingMetrics = scaler match {
    case s: ClusterScaler with ScalingStateProvider => Some(new ScalingMetrics(s, heartbeatsFn))
    case _ => None
  }

  // React to discovery deltas
  watcher.runForeach { state =>
    currentPods.set(state.current)

    // For added pods, start an SSE heartbeat. We only add to readyPods on first data event.
    state.added.foreach { pod =>
      startHeartBeatingWithQueryWorker(pod).failed.foreach { ex =>
        system.log.warning(s"Initial heartbeat stream failed for $pod: $ex")
      }
    }

    // For removed pods, drop them from ready set immediately
    state.removed.foreach { pod =>
      readyPods.updateAndGet(_ - pod)
    }

    val slotMap = mutable.HashMap.empty[Int, Pod]
    currentPods.get().zipWithIndex.foreach {
      case (pod, slot) =>
        slotMap.put(slot, pod)
    }
    podsBySlotId.set(slotMap.toMap)

    enforceBoundsAndMaybeScaleDown()
  }

  // Periodic visibility
  system.scheduler.scheduleWithFixedDelay(1.minute, 1.minute) { () =>
    logger.info(s"Known ready pods: ${readyPods.get().map(_.ip).mkString("[", ", ", "]")}")
  }(ec)

  // Periodic scale-down check
  system.scheduler.scheduleWithFixedDelay(scaleDownWaitMinutes.minutes, scaleDownWaitMinutes.minutes) { () =>
    enforceBoundsAndMaybeScaleDown()
  }(ec)

  /** Downward pressure + bounds enforcement. */
  private def enforceBoundsAndMaybeScaleDown(): Unit = {
    val discovered = currentPods.get.size

    if (discovered > maxWorkers) {
      scaleTo(maxWorkers);
      return
    }

    val idleMinutes = minutesSince(lastQueryTs)
    val sinceLastScaleMin = minutesSince(lastScaleTs)

    if (idleMinutes >= scaleDownWaitMinutes &&
      sinceLastScaleMin >= scaleDownWaitMinutes &&
      discovered > minWorkers) {
      scaleTo(minWorkers)
    }
  }

  /** Whether we are at (or beyond) desired max from the perspective of *heartbeating* workers. */
  def isScaledUp: Boolean = readyPods.get().size >= maxWorkers

  /** Record a query and (maybe) request a scale-up. */
  def recordQuery(): Unit = {
    lastQueryTs.set(System.currentTimeMillis())
    maybeScaleUp()
  }

  private def maybeScaleUp(): Unit = {
    val sinceLast = minutesSince(lastScaleTs)
    if (lastScaleTs.get == 0 || sinceLast >= scaleUpWaitMinutes) {
      scaleTo(maxWorkers)
    }
  }

  private def scaleTo(desired: Int): Unit = {
    val tgt = math.max(minWorkers, math.min(maxWorkers, desired))
    scaler.scaleTo(tgt)
    lastScaleTs.set(System.currentTimeMillis())
  }

  private def minutesSince(ref: AtomicLong): Long = {
    val ts = ref.get()
    if (ts == 0) 0L
    else Duration(System.currentTimeMillis() - ts, MILLISECONDS).toMinutes
  }

  /** Number of *heartbeating* workers. */
  def podCount: Int = readyPods.get().size

  /** Hash/affinity lookup -> heartbeating worker, if any. */
  def getWorkerFor(key: String): Option[Pod] = {
    val intToPod = podsBySlotId.get()
    val n = podCount
    if (n == 0 || intToPod == null || intToPod.isEmpty) return None
    val slotId = Math.floorMod(key.hashCode, n)
    intToPod.get(slotId).filter(readyPods.get().contains)
  }

  // -------------------- Heartbeat wiring (SSE) --------------------

  private def hostForUrl(ip: String): String =
    if (ip.contains(":")) s"[$ip]" else ip // bracket IPv6 literals

  /**
   * Start/maintain an SSE heartbeat stream to a worker.
   *
   * We add the pod to `readyPods` after the first SSE event arrives. If the stream
   * completes (EOF/connection drop), we remove the pod from `readyPods`.
   */
  private def startHeartBeatingWithQueryWorker(pod: Pod)
                                              (implicit system: ActorSystem, mat: Materializer): Future[Done] = {
    val ip   = pod.ip
    val port = QUERY_WORKER_PORT
    val uri  = s"http://${hostForUrl(ip)}:$port$QUERY_WORKER_HEARTBEAT"
    val req  = HttpRequest(uri = uri)

    logger.info(s"Discovered new query worker at $ip, attempting heartbeat")

    // Connect once and mark ready on first event
    def connectOnce(): Future[Unit] =
      Http().singleRequest(req).flatMap { resp =>
        resp.entity.dataBytes
          .via(EventStreamParser(Int.MaxValue, Int.MaxValue))
          .filter(_.data.nonEmpty)
          .take(1)
          .runWith(Sink.head)
          .map { _ =>
            readyPods.updateAndGet(_ + pod)
            logger.info(s"Heartbeat established for $ip")
          }
      }

    // One continuous SSE session (until it ends)
    def runSession(): Future[Done] =
      Http().singleRequest(req).flatMap { resp =>
        resp.entity.dataBytes
          .via(EventStreamParser(Int.MaxValue, Int.MaxValue))
          .map(_ => ())
          .runWith(Sink.ignore)
      }

    def loop(): Future[Done] = {
      val deadline = Deadline.now + 1.minute
      def retryInitial(): Future[Unit] =
        connectOnce().recoverWith {
          case _ if deadline.hasTimeLeft() => after(2.seconds, system.scheduler)(retryInitial())
        }

      retryInitial().flatMap(_ => runSession()).transformWith { _ =>
        // session ended; mark not ready and, if still discovered, try again
        readyPods.updateAndGet(_ - pod)
        if (currentPods.get().contains(pod)) {
          after(2.seconds, system.scheduler)(loop())
        } else {
          logger.info(s"Heartbeat loop stopping for $ip (no longer discovered)")
          Future.successful(Done)
        }
      }
    }

    loop()
  }
}

object WorkerManager {
  def apply()(implicit system: ActorSystem, mat: Materializer): WorkerManager = {
    val minWorkers = sys.env.getOrElse("NUM_MIN_QUERY_WORKERS", "2").toInt
    val maxWorkers = sys.env.getOrElse("NUM_MAX_QUERY_WORKERS", "30").toInt

    // These two are placeholders; in production, inject real routing/affinity lookup
    val watcher: Source[ClusterState, NotUsed] = ClusterWatcher.watch()
    val scaler: ClusterScaler = ClusterScaler.load()

    // Note: getHeartbeatingWorkers supplier is ignored by the ctor (we provide a correct internal one).
    new WorkerManager(
      watcher = watcher,
      minWorkers = minWorkers,
      maxWorkers = maxWorkers,
      scaler = scaler,
      isLegacyMode = false
    )
  }
}