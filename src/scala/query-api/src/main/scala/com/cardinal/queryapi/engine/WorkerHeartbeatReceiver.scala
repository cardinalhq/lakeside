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

import akka.actor.ActorSystem
import com.cardinal.discovery.Pod
import com.cardinal.model.{WorkerHeartbeat, WorkerStatus}
import com.cardinal.utils.Commons.WORKER_HEARTBEAT_TIMEOUT_SECONDS
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

@Component
class WorkerHeartbeatReceiver(implicit system: ActorSystem) {
  private implicit val ec: ExecutionContext = system.dispatcher
  private val logger = LoggerFactory.getLogger(getClass)

  private val activeWorkers = new ConcurrentHashMap[String, WorkerStatus]()
  private val heartbeatTimeout = WORKER_HEARTBEAT_TIMEOUT_SECONDS.seconds

  system.scheduler.scheduleWithFixedDelay(30.seconds, 30.seconds) { () =>
    cleanupStaleWorkers()
  }

  def registerHeartbeat(heartbeat: WorkerHeartbeat): Unit = {
    val workerStatus = WorkerStatus(
      pod = Pod(heartbeat.workerIp),
      lastSeen = heartbeat.timestamp,
      healthy = heartbeat.status == "healthy"
    )

    val wasNew = activeWorkers.put(heartbeat.workerIp, workerStatus) == null
    if (wasNew) {
      logger.info(s"New worker registered: ${heartbeat.workerIp}")
    }
  }

  def getActiveWorkers: Map[String, WorkerStatus] = {
    activeWorkers.asScala.toMap
  }

  def getReadyWorkerCount: Int = {
    activeWorkers.values.asScala.count(_.healthy)
  }

  def getWorkerFor(key: String): Option[Pod] = {
    val pods = activeWorkers.values.asScala
      .filter(_.healthy)
      .map(_.pod)
      .toSeq

    com.cardinal.discovery.Rendezvous.select[Pod](key, pods, _.ip)
  }

  private def cleanupStaleWorkers(): Unit = {
    val now = System.currentTimeMillis()
    val staleWorkers = activeWorkers.entrySet().asScala
      .filter(entry => now - entry.getValue.lastSeen > heartbeatTimeout.toMillis)
      .map(_.getKey)
      .toList

    staleWorkers.foreach { workerIp =>
      activeWorkers.remove(workerIp)
      logger.info(s"Removed stale worker: $workerIp")
    }

    if (staleWorkers.nonEmpty) {
      logger.info(s"Active workers: ${getReadyWorkerCount}")
    }
  }
}