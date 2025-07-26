package com.cardinal.config

import com.cardinal.model.StorageProfile
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.io.FileInputStream
import java.util.UUID
import scala.util.Using

case class RawStorageProfile(
                              @JsonProperty("organization_id") organizationId: String,
                              @JsonProperty("instance_num") instanceNum: Int,
                              @JsonProperty("collector_name") collectorName: String,
                              @JsonProperty("cloud_provider") cloudProvider: String,
                              @JsonProperty("region") region: String,
                              @JsonProperty("role") role: Option[String],
                              @JsonProperty("bucket") bucket: String,
                              @JsonProperty("hosted") hosted: Option[Boolean],
                              @JsonProperty("endpoint") endpoint: Option[String],
                              @JsonProperty("insecure_tls") insecureTLS: Option[Boolean],
                              @JsonProperty("use_path_style") usePathStyle: Option[Boolean],
                              @JsonProperty("use_ssl") useSsl: Option[Boolean],
                            )


object StorageProfileCacheFile {
  private val mapper = new ObjectMapper(new YAMLFactory())
    .registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def fromFile(filePath: String): StorageProfileCacheFile = {
    val profiles = Using.resource(new FileInputStream(filePath)) { in =>
      val raw = mapper.readValue(in, new TypeReference[List[RawStorageProfile]] {})
      toStorageProfiles(raw)
    }

    new StorageProfileCacheFile(profiles)
  }

  def fromYamlString(yaml: String): StorageProfileCacheFile = {
    val raw = mapper.readValue(yaml, new TypeReference[List[RawStorageProfile]] {})
    val profiles = toStorageProfiles(raw)

    new StorageProfileCacheFile(profiles)
  }

  private def toStorageProfiles(rawList: List[RawStorageProfile]): List[StorageProfile] =
    rawList.map { raw =>
      val role = raw.role.getOrElse("")
      val hosted = raw.hosted.getOrElse(role.isEmpty)

      StorageProfile(
        collectorUuid = None,
        collectorId = Some(raw.collectorName),
        instanceNum = Some(raw.instanceNum),
        organizationId = raw.organizationId,
        storageProfileId = UUID.randomUUID().toString,
        bucket = raw.bucket,
        cloudProvider = raw.cloudProvider,
        region = raw.region,
        hosted = hosted,
        spProperties = Map.empty,
        role = role,
        collectorType = 0,
        endpoint = raw.endpoint,
        useSsl = raw.useSsl.getOrElse(true)
      )
    }
}

class StorageProfileCacheFile private (initialProfiles: List[StorageProfile])
  extends StorageProfileCache {

  @volatile private var profiles: List[StorageProfile] = initialProfiles

  override def getStorageProfile(bucket: String): Option[StorageProfile] =
    profiles.find(_.bucket == bucket)

  override def getStorageProfile(orgId: String, collectorId: String, bucket: String): Option[StorageProfile] =
    profiles.find(p => p.organizationId == orgId && p.collectorId.contains(collectorId) && p.bucket == bucket)

  override def getStorageProfile(orgId: String, instanceNum: Int): Option[StorageProfile] =
    profiles.find(p => p.organizationId == orgId && p.instanceNum.contains(instanceNum))

  override def getStorageProfilesByOrgId(orgId: String): List[StorageProfile] =
    profiles.filter(_.organizationId == orgId)
}
