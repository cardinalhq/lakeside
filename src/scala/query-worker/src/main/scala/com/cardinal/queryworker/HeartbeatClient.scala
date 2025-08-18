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

package com.cardinal.queryworker

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import com.cardinal.model.WorkerHeartbeat
import com.cardinal.utils.Commons._
import com.netflix.atlas.json.Json
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

import java.net.{InetAddress, NetworkInterface}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

@Component
class HeartbeatClient(implicit system: ActorSystem) {
  private implicit val ec: ExecutionContext = system.dispatcher
  private val logger = LoggerFactory.getLogger(getClass)

  private val queryApiEndpoint = sys.env.getOrElse("QUERY_API_ENDPOINT", "http://localhost:8080")
  private val authToken = sys.env.getOrElse("QUERY_API_AUTH_TOKEN", "")
  private val heartbeatInterval = WORKER_HEARTBEAT_INTERVAL_SECONDS.seconds

  def startHeartbeating(): Unit = {
    if (authToken.isEmpty) {
      logger.warn("QUERY_API_AUTH_TOKEN not set, heartbeating disabled")
      return
    }

    val localIp = getLocalIP()
    logger.info(s"Starting heartbeat to $queryApiEndpoint with IP $localIp")

    system.scheduler.scheduleWithFixedDelay(
      initialDelay = 5.seconds,
      delay = heartbeatInterval
    ) { () =>
      sendHeartbeat(localIp)
    }
  }

  private def sendHeartbeat(localIp: String): Unit = {
    val heartbeat = WorkerHeartbeat(
      workerIp = localIp,
      port = QUERY_WORKER_PORT,
      timestamp = System.currentTimeMillis(),
      status = "healthy"
    )

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = s"$queryApiEndpoint$WORKER_HEARTBEAT_ENDPOINT",
      headers = List(RawHeader("Authorization", s"Bearer $authToken")),
      entity = HttpEntity(ContentTypes.`application/json`, Json.encode(heartbeat))
    )

    Http().singleRequest(request).onComplete {
      case Success(response) =>
        if (response.status.isSuccess()) {
          logger.debug(s"Heartbeat sent successfully")
        } else {
          logger.warn(s"Heartbeat failed with status: ${response.status}")
        }
        response.discardEntityBytes()
      case Failure(ex) =>
        logger.warn(s"Heartbeat request failed: ${ex.getMessage}")
    }
  }

  private def getLocalIP(): String = {
    try {
      NetworkInterface.getNetworkInterfaces.asScala
        .filter(_.isUp)
        .filter(!_.isLoopback)
        .flatMap(_.getInetAddresses.asScala)
        .find(addr => !addr.isLoopbackAddress && addr.isSiteLocalAddress)
        .map(_.getHostAddress)
        .getOrElse(InetAddress.getLocalHost.getHostAddress)
    } catch {
      case _: Exception => "unknown"
    }
  }
}