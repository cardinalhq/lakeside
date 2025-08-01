package com.cardinal.objectstorage

trait ObjectStore {
  def putObject(bucketName: String, key: String, file: java.io.File): Unit
  def downloadObject(bucketName: String, key: String): (Boolean, Long)
  def downloadAllObjects(bucketName: String, prefix: String, localDestination: String): Long
  def moveObject(bucketName: String, fromKey: String, toKey: String): Boolean
  def deleteObjects(bucketName: String, keys: Set[String]): Unit
}
