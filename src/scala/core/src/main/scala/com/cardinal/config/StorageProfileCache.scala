package com.cardinal.config

import com.cardinal.model.StorageProfile

trait StorageProfileCache {
  def getStorageProfile(bucket: String): Option[StorageProfile]

  def getStorageProfile(orgId: String, collectorId: String, bucket: String): Option[StorageProfile]

  def getStorageProfile(orgId: String, instanceNum: Int): Option[StorageProfile]

  def getStorageProfilesByOrgId(orgId: String): List[StorageProfile]
}
