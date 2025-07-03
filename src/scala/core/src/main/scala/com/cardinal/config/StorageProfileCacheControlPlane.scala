package com.cardinal.config

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.unmarshalling.sse.EventStreamParser
import akka.stream.RestartSettings
import akka.stream.scaladsl.{RestartSource, Source}
import com.cardinal.auth.AuthToken
import com.cardinal.model.StorageProfile
import com.cardinal.utils.Commons.{CARDINAL_REGIONAL_DEPLOYMENT_ID, CONTROL_PLANE_HOST}
import com.cardinal.utils.ControlPlaneEndpoints.STORAGE_PROFILES_ENDPOINT
import com.netflix.atlas.json.Json
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class StorageProfileCacheControlPlane(actorSystem: ActorSystem) extends StorageProfileCache  {

  implicit val as: ActorSystem = actorSystem
  implicit val ec: ExecutionContext = as.dispatcher

  private val logger = LoggerFactory.getLogger(getClass)

  private val cacheByOrgCollectorBucket = new AtomicReference[Map[String, StorageProfile]](Map.empty)
  private val cacheByBucket = new AtomicReference[Map[String, StorageProfile]](Map.empty)
  private val cacheByOrgIdInstanceNum = new AtomicReference[Map[(String, Int), StorageProfile]](Map.empty)
  private val storageProfilesByOrgId = new AtomicReference[Map[String, List[StorageProfile]]](Map.empty)

  def getStorageProfile(bucket: String): Option[StorageProfile] = {
    val map = cacheByBucket.get()
    if (map == null) None
    else {
      map.get(bucket)
    }
  }

  def getStorageProfile(orgId: String, collectorId: String, bucket: String): Option[StorageProfile] = {
    val map = cacheByOrgCollectorBucket.get()
    if (map == null) None
    else {
      map.get(mkId(orgId, collectorId, bucket))
    }
  }

  def getStorageProfile(orgId: String, instanceNum: Int): Option[StorageProfile] = {
    val map = cacheByOrgIdInstanceNum.get()
    if (map == null) None
    else {
      map.get((orgId, instanceNum))
    }
  }

  def getStorageProfilesByOrgId(orgId: String): List[StorageProfile] = {
    val map = storageProfilesByOrgId.get()
    if (map == null) List.empty
    else {
      map.getOrElse(orgId, List.empty)
    }
  }

  private def mkId(orgId: String, collectorId: String, bucket: String): String = {
    s"$orgId-$collectorId-$bucket"
  }

  def start(): Unit = {
    RestartSource
      .withBackoff(RestartSettings(1.seconds, 3.seconds, 0.3))(() => {
        Source
          .single(
            HttpRequest(
              uri = STORAGE_PROFILES_ENDPOINT,
              headers = AuthToken.getAuthHeader(CARDINAL_REGIONAL_DEPLOYMENT_ID)
            )
          )
          .via(
            if (CONTROL_PLANE_HOST.startsWith("localhost")) {
              val hostParts = CONTROL_PLANE_HOST.split(":")
              Http().outgoingConnection(host = hostParts(0), port = hostParts(1).toInt)
            } else {
              Http().outgoingConnectionHttps(host = CONTROL_PLANE_HOST)
            }
          )
          .flatMapConcat(response => response.entity.dataBytes)
          .via(EventStreamParser(maxLineSize = Int.MaxValue, maxEventSize = Int.MaxValue))
          .map {
            sse =>
              val incomingPayload = ujson.read(sse.data)
              try {
                incomingPayload("type").str match {
                  case "heartbeat" =>
                    None
                  case _ =>
                    val profiles = Json.decode[Map[String, StorageProfile]](ujson.write(incomingPayload("message")))
                    cacheByOrgCollectorBucket.set(profiles)

                    val mapByInstanceNum = mutable.Map.empty[(String, Int), StorageProfile]
                    val mapByBucket = mutable.Map.empty[String, StorageProfile]
                    val mapByOrgId = mutable.Map.empty[String, ListBuffer[StorageProfile]]

                    profiles.values.foreach(sp => {
                      //only placing storage profiles linked to a collector
                      if (sp.instanceNum.nonEmpty){
                        mapByInstanceNum.put((sp.organizationId, sp.instanceNum.get), sp)
                      }
                      //only putting native buckets in this map.
                      // if SAAS, cardinal should have one storage profile per saas bucket, pull info from that SP
                      if (!sp.hosted){
                        mapByBucket.put(sp.bucket, sp)
                      }
                      //only placing storage profiles linked to a collector
                      if (sp.instanceNum.nonEmpty) {
                        mapByOrgId.getOrElseUpdate(sp.organizationId, ListBuffer.empty) += sp
                      }
                    })

                    cacheByOrgIdInstanceNum.set(mapByInstanceNum.toMap)
                    cacheByBucket.set(mapByBucket.toMap)
                    storageProfilesByOrgId.set(mapByOrgId.map(kv => kv._1 -> kv._2.toList).toMap)
                    Some("")
                }
              } catch {
                case e: Exception =>
                  logger.error("Error applying update", e)
                  Some("")
              }
          }
      })
      .runForeach { _ =>
        }
      .recover {
        case e: Exception =>
          logger.error(s"Stream failed with exception ${e.getMessage}", e)
      }

    while (cacheByOrgIdInstanceNum.get().isEmpty || storageProfilesByOrgId.get().isEmpty || cacheByOrgCollectorBucket
             .get()
             .isEmpty) {
      logger.info("Waiting for StorageProfileCache to get populated...")
      Thread.sleep(1000)
    }
    logger.info("Done waiting for storage profiles...")
    // Logging the profiles in cacheByOrgCollectorBucket
    val profiles = cacheByBucket.get()
    if (profiles.nonEmpty) {
      logger.info(s"Profiles in cacheByBucket: ${profiles.mkString(", ")}")
    } else {
      logger.info("No profiles found in cacheByBucket.")
    }
  }
}


