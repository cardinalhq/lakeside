package com.cardinal.objectstorage

import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.{Files, Paths}

abstract class BaseObjectStore extends ObjectStore {

  private val logger = LoggerFactory.getLogger(classOf[BaseObjectStore])

  private def createDestinationFile(key: String): File = {
    val destinationFile = new File(key)
    if (destinationFile.getParentFile != null) {
      destinationFile.getParentFile.mkdirs() // Create parent directories if they do not exist
    }
    destinationFile
  }

  override def putObject(bucketName: String, key: String, file: File): Unit = {
    logger.debug(s"Putting object $key into bucket $bucketName")
    _putObject(bucketName, key, file)
  }
  def _putObject(bucketName: String, key: String, file: File): Unit

  override def downloadObject(bucketName: String, key: String): (Boolean, Long) = {
    logger.debug(s"Downloading object $key from bucket $bucketName")
    logger.trace(s"Current working directory: {}", System.getProperty("user.dir"))
    val destinationFile = createDestinationFile(key)
    _downloadObject(bucketName, key, destinationFile)

    logger.info(
      s"Download object: Bucket: $bucketName  key: $key result: ${destinationFile.exists()} Size: ${destinationFile.length()} bytes."
    )
    (destinationFile.exists(), destinationFile.length())
  }
  def _downloadObject(bucketName: String, key: String, destinationFile: File): Unit

  override def downloadAllObjects(bucketName: String, prefix: String, localDestination: String): Long = {
    val restoreFolderPath = Paths.get(localDestination)
    if (!Files.exists(restoreFolderPath)) {
      Files.createDirectory(restoreFolderPath)
    }
    logger.debug(s"Downloading all objects from bucket $bucketName with prefix $prefix to $localDestination")
    _downloadAllObjects(bucketName, prefix, localDestination)
  }
  def _downloadAllObjects(bucketName: String, prefix: String, localDestination: String): Long

  override def moveObject(bucketName: String, fromKey: String, toKey: String): Boolean = {
    logger.debug(s"Moving object from $fromKey to $toKey in bucket $bucketName")
    _moveObject(bucketName, fromKey, toKey)
  }
  def _moveObject(bucketName: String, fromKey: String, toKey: String): Boolean

  override def deleteObjects(bucketName: String, keys: Set[String]): Unit = {
    logger.debug(s"Deleting objects from bucket $bucketName")
    _deleteObjects(bucketName, keys)
  }
  def _deleteObjects(bucketName: String, keys: Set[String]): Unit
}
