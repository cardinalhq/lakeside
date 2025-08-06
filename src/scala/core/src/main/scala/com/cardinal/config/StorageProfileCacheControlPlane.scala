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

package com.cardinal.config

import akka.actor.ActorSystem
import com.cardinal.dbutils.DBDataSources.getConfigSource
import com.cardinal.model.StorageProfile
import com.netflix.atlas.json.Json
import org.slf4j.LoggerFactory

import java.sql.{Connection, Statement}
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.control.NonFatal

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

  private def loadProfilesFromDb(): Seq[StorageProfile] = {
    val profiles = List.newBuilder[StorageProfile]
    var conn: Connection    = null
    var stmt: Statement     = null

    val sql = "SELECT c.id as collector_uuid, c.external_id as collector_id, c.instance_num as instance_num," +
      " sp.organization_id as organization_id, sp.id as storage_profile_id," +
      " sp.bucket as bucket, sp.cloud_provider as cloud_provider, sp.region as region, sp.hosted as hosted, sp.properties as storage_profile_properties, sp.role as role," +
      " c.type as type" +
      " FROM c_storage_profiles sp left outer join c_collectors c on   c.storage_profile_id = sp.id where c.deleted_at is null"

    try {
      conn  = getConfigSource.getConnection
      stmt  = conn.createStatement()
      val rs = stmt.executeQuery(sql)
      while (rs.next()) {
        val collectorUuid  = Option(rs.getString("collector_uuid"))
        val collectorId    = Option(rs.getString("collector_id"))
        val instanceNumRaw = rs.getInt("instance_num")
        val instanceNum    = if (rs.wasNull()) None else Some(instanceNumRaw)

        // … pull out the rest of the columns …
        val spPropertiesJson = rs.getString("storage_profile_properties")
        val spProperties     = Json.decode[Map[String, Object]](spPropertiesJson)

        profiles += StorageProfile(
          collectorUuid, collectorId, instanceNum,
          rs.getString("organization_id"),
          rs.getString("storage_profile_id"),
          rs.getString("bucket"),
          rs.getString("cloud_provider"),
          rs.getString("region"),
          rs.getBoolean("hosted"),
          spProperties,
          rs.getString("role"),
          rs.getInt("type"),
          endpoint = None
        )
      }
      rs.close()
      profiles.result()
    } catch {
      case NonFatal(e) =>
        logger.error("Error polling storage profiles from DB", e)
        Seq.empty
    } finally {
      if (stmt != null) stmt.close()
      if (conn  != null) conn.close()
    }
  }

  /** Given a flat Seq of profiles, rebuild all of your cache maps. */
  private def refreshCaches(profiles: Seq[StorageProfile]): Unit = {
    val byOrgColBucket = mutable.Map.empty[String, StorageProfile]
    val byBucket       = mutable.Map.empty[String, StorageProfile]
    val byOrgIdInst    = mutable.Map.empty[(String, Int), StorageProfile]
    val byOrgIdList    = mutable.Map.empty[String, ListBuffer[StorageProfile]]

    profiles.foreach { sp =>
      sp.instanceNum.foreach(i => byOrgIdInst((sp.organizationId, i)) = sp)
      if (!sp.hosted)                   byBucket(sp.bucket) = sp
      sp.instanceNum.foreach { _ =>
        val buf = byOrgIdList.getOrElseUpdate(sp.organizationId, ListBuffer.empty)
        buf += sp
      }
      sp.collectorId.foreach { col =>
        val key = s"${sp.organizationId}-$col-${sp.bucket}"
        byOrgColBucket(key) = sp
      }
    }

    cacheByOrgCollectorBucket.set(byOrgColBucket.toMap)
    cacheByBucket.set(byBucket.toMap)
    cacheByOrgIdInstanceNum.set(byOrgIdInst.toMap)
    storageProfilesByOrgId.set(byOrgIdList.view.mapValues(_.toList).toMap)
  }

  /** Kick off a repeating poll at a fixed interval. */
  def start(pollInterval: FiniteDuration = 1.minute): Unit = {
    actorSystem.scheduler.scheduleWithFixedDelay(0.seconds, pollInterval) { () =>
      val profiles = loadProfilesFromDb()
      if (profiles.nonEmpty) {
        refreshCaches(profiles)
        logger.info(s"Loaded ${profiles.size} storage profiles from DB")
      } else {
        logger.warn("StorageProfile load returned 0 rows; keeping previous cache")
      }
    }
  }
}


