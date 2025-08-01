package com.cardinal.objectstorage

import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Files

class LocalObjectStore(rootDir: String) extends BaseObjectStore {
  private val logger = LoggerFactory.getLogger(getClass)
  logger.info(s"Instantiating local object store pointing to $rootDir")

  private def getBucketPath(bucketName: String): File = new File(s"$rootDir/$bucketName")

  private def getObjectPath(bucketName: String, key: String): File = new File(s"$rootDir/$bucketName/$key")

  override def _downloadObject(bucketName: String, key: String, destinationFile: File): Unit = {
    val file = getObjectPath(bucketName, key)
    if (file.exists() && file.isFile) {
      logger.info(
        s"Object $key downloaded from bucket $bucketName successfully. Size: ${file.length()} bytes."
      )
      Files.copy(file.toPath, destinationFile.toPath)
    } else {
      logger.error(s"Error downloading $key from bucket file does not exist")
    }
  }

  override def _downloadAllObjects(bucketName: String, prefix: String, localDestination: String): Long = {
    val bucket = getBucketPath(bucketName)
    if (!bucket.exists()) return 0L

    val files = bucket.listFiles().filter(_.getName.startsWith(prefix))
    val destDir = new File(localDestination)
    if (!destDir.exists()) destDir.mkdirs()

    files.foldLeft(0L) { (totalSize, file) =>
      val destFile = new File(s"$localDestination/${file.getName}")
      Files.copy(file.toPath, destFile.toPath)
      logger.info(s"Downloaded  objects from bucket $bucketName with prefix $prefix.")
      totalSize + file.length()
    }
  }
}
