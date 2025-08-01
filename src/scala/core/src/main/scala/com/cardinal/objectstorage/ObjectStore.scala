package com.cardinal.objectstorage

trait ObjectStore {
  def downloadObject(bucketName: String, key: String): (Boolean, Long)
  def downloadAllObjects(bucketName: String, prefix: String, localDestination: String): Long
}
