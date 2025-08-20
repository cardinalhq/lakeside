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
import com.cardinal.instrumentation.Metrics.gauge
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class ScalingMetrics(
  scaler: ClusterScaler with ScalingStateProvider,
  getHeartbeatingWorkers: () => Int
)(implicit system: ActorSystem) {
  private implicit val ec: ExecutionContext = system.dispatcher
  private val logger = LoggerFactory.getLogger(getClass)

  private val desiredWorkers = new AtomicInteger(0)
  private val heartbeatingWorkers = new AtomicInteger(0)
  private val k8sReportedWorkers = new AtomicInteger(0)

  system.scheduler.scheduleWithFixedDelay(15.seconds, 15.seconds) { () =>
    updateMetrics()
  }

  def updateMetrics(): Unit = {
    try {
      val desired = scaler.getCurrentDesiredReplicas()
      val heartbeating = getHeartbeatingWorkers()
      val k8sReported = scaler.getCurrentPodCount()

      desiredWorkers.set(desired)
      heartbeatingWorkers.set(heartbeating)
      k8sReportedWorkers.set(k8sReported)

      gauge("workers.desired", desired)
      gauge("workers.heartbeating", heartbeating)
      gauge("workers.k8s_reported", k8sReported)
      gauge("workers.scaling_gap", math.max(0, desired - heartbeating))

      if (desired != heartbeating || desired != k8sReported) {
        logger.info(s"Scaling metrics: desired=$desired, heartbeating=$heartbeating, k8s_reported=$k8sReported")
      }
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to update scaling metrics: ${ex.getMessage}")
    }
  }

  def getDesiredWorkers: Int = desiredWorkers.get()
  def getHeartbeatingWorkers: Int = heartbeatingWorkers.get()
  def getK8sReportedWorkers: Int = k8sReportedWorkers.get()
}