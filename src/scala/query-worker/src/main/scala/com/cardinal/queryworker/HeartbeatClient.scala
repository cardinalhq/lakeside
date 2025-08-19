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
import akka.stream.Materializer
import com.cardinal.discovery.{KubernetesWatcher, Pod}
import com.cardinal.model.WorkerHeartbeat
import com.cardinal.utils.Commons._
import com.cardinal.utils.EnvUtils
import com.netflix.atlas.json.Json
import org.slf4j.LoggerFactory

import java.net.{InetAddress, NetworkInterface}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

class HeartbeatClient(implicit system: ActorSystem) {
  private implicit val ec: ExecutionContext = system.dispatcher
  private implicit val mat: Materializer = Materializer(system)
  private val logger = LoggerFactory.getLogger(getClass)

  private val authToken = sys.env.getOrElse("TOKEN_HMAC256_KEY", {
    throw new IllegalStateException("TOKEN_HMAC256_KEY environment variable is required but not set")
  })
  private val heartbeatInterval = WORKER_HEARTBEAT_INTERVAL_SECONDS.seconds
  private val queryApiPort = sys.env.getOrElse("QUERY_API_PORT", "7101").toInt
  private val deploymentType = sys.env.getOrElse("QUERY_API_DEPLOYMENT_TYPE", "deployment").toLowerCase
  
  // Track discovered API endpoints
  private val currentApiEndpoints = new AtomicReference[Set[Pod]](Set.empty)

  def startHeartbeating(): Unit = {
    val localIp = getLocalIP()
    
    // Check if we're in Kubernetes environment for service discovery
    val executionEnv = sys.env.getOrElse("EXECUTION_ENVIRONMENT", "local")
    
    if (executionEnv == "kubernetes") {
      startKubernetesDiscovery(localIp)
    } else {
      // Fallback to single endpoint for local/testing
      val queryApiEndpoint = sys.env.getOrElse("QUERY_API_ENDPOINT", {
        throw new IllegalStateException("QUERY_API_ENDPOINT environment variable is required but not set")
      })
      logger.info(s"Starting heartbeat to single endpoint $queryApiEndpoint with IP $localIp")
      startSingleEndpointHeartbeat(queryApiEndpoint, localIp)
    }
  }
  
  private def startKubernetesDiscovery(localIp: String): Unit = {
    logger.info(s"Starting Kubernetes-based API discovery for worker IP $localIp (deployment type: $deploymentType)")
    
    // Validate deployment type
    deploymentType match {
      case "deployment" | "statefulset" => // Valid types
      case other => throw new IllegalArgumentException(s"Invalid QUERY_API_DEPLOYMENT_TYPE: $other. Must be 'deployment' or 'statefulset'")
    }
    
    // Get query-api service labels and namespace
    val queryApiLabels = getQueryApiServiceLabels()
    val namespace = EnvUtils.mustFirstEnv(Seq("QUERY_API_NAMESPACE", "POD_NAMESPACE"))
    
    // Start watching for query-api endpoints based on deployment type
    val discoverySource = deploymentType match {
      case "deployment" =>
        logger.info("Using Deployment-based service discovery via EndpointSlices")
        // Regular Deployments typically use ClusterIP services
        // EndpointSlices will contain all pod IPs backing the service
        KubernetesWatcher.startWatching(queryApiLabels, namespace)
        
      case "statefulset" =>
        logger.info("Using StatefulSet-based service discovery via EndpointSlices")
        // StatefulSets can use either:
        // 1. Headless services (clusterIP: None) - individual pod DNS names
        // 2. Regular services - same as Deployment
        // EndpointSlices work for both, containing individual pod IPs
        // The label selector should match the StatefulSet's service labels
        KubernetesWatcher.startWatching(queryApiLabels, namespace)
    }
    
    discoverySource.runForeach { state =>
      val oldEndpoints = currentApiEndpoints.getAndSet(state.current)
      if (state.current != oldEndpoints) {
        logger.info(s"API endpoints updated (${deploymentType}): ${state.current.map(_.ip).mkString("[", ", ", "]")}")
      }
    }
    
    // Start periodic heartbeating to all discovered endpoints
    system.scheduler.scheduleWithFixedDelay(
      initialDelay = 5.seconds,
      delay = heartbeatInterval
    ) { () =>
      sendHeartbeatsToAllEndpoints(localIp)
    }
  }
  
  private def startSingleEndpointHeartbeat(endpoint: String, localIp: String): Unit = {
    system.scheduler.scheduleWithFixedDelay(
      initialDelay = 5.seconds,
      delay = heartbeatInterval
    ) { () =>
      sendHeartbeatToEndpoint(endpoint, localIp)
    }
  }
  
  private def getQueryApiServiceLabels(): Map[String, String] = {
    // Get query-api service labels from environment variables
    val rawLabels = EnvUtils.mustFirstEnv(Seq("QUERY_API_LABEL_SELECTOR", "API_SERVICE_LABEL_SELECTOR"))
    EnvUtils.parseLabels(rawLabels)
  }
  
  private def sendHeartbeatsToAllEndpoints(localIp: String): Unit = {
    val endpoints = currentApiEndpoints.get()
    if (endpoints.isEmpty) {
      logger.warn("No API endpoints discovered yet, skipping heartbeat")
      return
    }
    
    endpoints.foreach { pod =>
      val endpoint = s"http://${pod.ip}:$queryApiPort"
      sendHeartbeatToEndpoint(endpoint, localIp)
    }
  }

  private def sendHeartbeatToEndpoint(endpoint: String, localIp: String): Unit = {
    val heartbeat = WorkerHeartbeat(
      workerIp = localIp,
      port = QUERY_WORKER_PORT,
      timestamp = System.currentTimeMillis(),
      status = "healthy"
    )

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = s"$endpoint$WORKER_HEARTBEAT_ENDPOINT",
      headers = List(RawHeader("Authorization", s"Bearer $authToken")),
      entity = HttpEntity(ContentTypes.`application/json`, Json.encode(heartbeat))
    )

    Http().singleRequest(request).onComplete {
      case Success(response) =>
        if (response.status.isSuccess()) {
          logger.debug(s"Heartbeat sent successfully to $endpoint")
        } else {
          logger.warn(s"Heartbeat failed to $endpoint with status: ${response.status}")
        }
        response.discardEntityBytes()
      case Failure(ex) =>
        logger.warn(s"Heartbeat request failed to $endpoint: ${ex.getMessage}")
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
      case ex: Exception =>
        logger.warn(s"Failed to determine local IP address: ${ex.getMessage}")
        "unknown"
    }
  }
}