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
}
