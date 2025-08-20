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

import com.amazonaws.services.s3.model._
import com.cardinal.instrumentation.Metrics.{gauge, recordExecutionTime}
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Files
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

class S3Store(s3ClientCache: S3ClientCache) extends BaseObjectStore {
  private val logger = LoggerFactory.getLogger(getClass)

  override def _downloadObject(bucketName: String, key: String, destinationFile: File): Unit = {
    try {
      val metadata =
        s3ClientCache.getClient(bucketName).getObject(new GetObjectRequest(bucketName, key), destinationFile)
      logger.info(
        s"Object $key downloaded from bucket $bucketName successfully. Size: ${metadata.getContentLength} bytes."
      )
    } catch {
      case e: AmazonS3Exception if e.getStatusCode == 404 =>
        logger.trace(s"Object $key not found in bucket $bucketName. Error: ${e.getMessage}")
      case e: Exception =>
        logger.error(s"Error downloading $key from bucket $bucketName", e)
    }
  }

  override def _downloadAllObjects(bucketName: String, prefix: String, localDestination: String): Long = {
    try {
      val listObjectsRequest = new ListObjectsRequest()
      listObjectsRequest.setBucketName(bucketName)
      listObjectsRequest.setPrefix(prefix)

      var objectListing = s3ClientCache.getClient(bucketName).listObjects(listObjectsRequest)

      val objectKeys = ListBuffer[String]()
      var keepGoing = true
      while (keepGoing) {
        val summaries: java.util.List[S3ObjectSummary] = objectListing.getObjectSummaries
        summaries.forEach(summary => objectKeys += summary.getKey)

        if (objectListing.isTruncated) {
          objectListing = s3ClientCache.getClient(bucketName).listNextBatchOfObjects(objectListing)
        } else {
          keepGoing = false
        }
      }

      objectKeys
        .result()
        .foreach(key => {
          if (key != prefix) {
            val localFile = key.replace(prefix, s"$localDestination/")
            s3ClientCache.getClient(bucketName).getObject(new GetObjectRequest(bucketName, key), new File(localFile))
          }
        })

      logger.info(s"Downloaded ${objectKeys.size} objects from bucket $bucketName with prefix $prefix.")
      objectKeys.size
    } catch {
      case e: Exception =>
        logger.error(s"Error downloading all objects from bucket $bucketName with prefix $prefix", e)
        throw e
    }
  }
}
