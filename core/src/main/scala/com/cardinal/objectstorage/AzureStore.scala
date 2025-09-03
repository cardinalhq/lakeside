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

import com.azure.core.exception.ResourceNotFoundException
import com.azure.storage.blob.models.ListBlobsOptions
import org.slf4j.LoggerFactory

import java.io.File
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

class AzureStore(azureClientCache: AzureClientCache) extends BaseObjectStore {
  private val logger = LoggerFactory.getLogger(getClass)

  override def _downloadObject(containerName: String, key: String, destinationFile: File): Unit = {
    try {
      val blobClient = azureClientCache.getBlobClient(containerName, key)
      blobClient.downloadToFile(destinationFile.getPath, true) // overwrite if exists
      
      val properties = blobClient.getProperties
      logger.info(
        s"Object $key downloaded from container $containerName successfully. Size: ${properties.getBlobSize} bytes."
      )
    } catch {
      case e: ResourceNotFoundException =>
        logger.trace(s"Object $key not found in container $containerName. Error: ${e.getMessage}")
      case e: Exception =>
        logger.error(s"Error downloading $key from container $containerName", e)
        throw e
    }
  }

  override def _downloadAllObjects(containerName: String, prefix: String, localDestination: String): Long = {
    try {
      val containerClient = azureClientCache.getContainerClient(containerName)
      
      // List blobs with prefix
      val listOptions = new ListBlobsOptions().setPrefix(prefix)
      val blobs = containerClient.listBlobs(listOptions, null).asScala.toList
      
      var downloadedCount = 0L
      
      blobs.foreach { blobItem =>
        val blobName = blobItem.getName
        if (!blobName.endsWith("/")) { // Skip directory placeholders
          val localFileName = blobName.replace(prefix, "")
          val localFile = new File(s"$localDestination/$localFileName")
          
          // Create parent directories if they don't exist
          if (localFile.getParentFile != null) {
            localFile.getParentFile.mkdirs()
          }
          
          val blobClient = containerClient.getBlobClient(blobName)
          blobClient.downloadToFile(localFile.getPath, true) // overwrite if exists
          downloadedCount += 1
        }
      }
      
      logger.info(s"Downloaded $downloadedCount objects from container $containerName with prefix $prefix.")
      downloadedCount
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error downloading all objects from container $containerName with prefix $prefix", e)
        throw e
    }
  }
}