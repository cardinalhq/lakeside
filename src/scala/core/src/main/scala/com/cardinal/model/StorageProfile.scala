package com.cardinal.model

case class StorageProfile(
  collectorUuid: Option[String],
  collectorId: Option[String],
  instanceNum: Option[Int],
  organizationId: String,
  storageProfileId: String,
  bucket: String,
  cloudProvider: String,
  region: String,
  hosted: Boolean,
  spProperties: Map[String, Object],
  role: String,
  collectorType: Int,
  endpoint : Option[String],
  useSsl: Boolean = true
)
