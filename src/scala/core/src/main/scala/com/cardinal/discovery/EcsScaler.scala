package com.cardinal.discovery

import software.amazon.awssdk.services.ecs.EcsClient
import software.amazon.awssdk.services.ecs.model.{UpdateServiceRequest, DescribeServicesRequest}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger

/**
 * A WorkerScaler backed by AWS ECS.
 *
 * @param clusterName   the ECS cluster name where your service is running
 * @param serviceName   the ECS service name to scale
 */
class EcsScaler(clusterName: String, serviceName: String) extends WorkerScaler {
  private val logger = LoggerFactory.getLogger(getClass)

  private val ecs: EcsClient = EcsClient.builder().build()
  private val lastDesired = new AtomicInteger(0)

  override def scaleTo(desiredReplicas: Int): Unit = {
    try {
      val req = UpdateServiceRequest.builder()
        .cluster(clusterName)
        .service(serviceName)
        .desiredCount(desiredReplicas)
        .build()

      ecs.updateService(req)
      logger.info(s"[ECS] scaleTo($desiredReplicas) requested for service '$serviceName' in cluster '$clusterName'")
      lastDesired.set(desiredReplicas)

    } catch {
      case e: Exception =>
        logger.error(s"[ECS] Error scaling service '$serviceName' to $desiredReplicas", e)
    }
  }
}

object EcsScaler {
  def apply(clusterName: String, serviceName: String): EcsScaler =
    new EcsScaler(clusterName, serviceName)
}
