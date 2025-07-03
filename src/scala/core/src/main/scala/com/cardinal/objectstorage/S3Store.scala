package com.cardinal.objectstorage

import com.amazonaws.services.s3.model._
import com.cardinal.instrumentation.Metrics.{gauge, recordExecutionTime}
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Files
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

trait Retryable {
  def withRetries[T](maxAttempts: Int, baseBackoffMs: Long)(block: => T): T = {
    var attempt = 0
    var lastError: Throwable = null

    while (attempt < maxAttempts) {
      try {
        return block
      } catch {
        case NonFatal(e) =>
          lastError = e
          attempt += 1
          if (attempt < maxAttempts) {
            val backoff = baseBackoffMs * math.pow(2, attempt - 1).toLong
            Thread.sleep(backoff)
          }
      }
    }
    throw new RuntimeException(s"Operation failed after $maxAttempts attempts", lastError)
  }
}

class S3Store(s3ClientCache: S3ClientCache) extends BaseObjectStore with Retryable {
  private val logger = LoggerFactory.getLogger(getClass)

  def _putObject(bucketName: String, key: String, file: File): Unit = {
    withRetries(maxAttempts = 10, baseBackoffMs = 500) {
      val objPutStart = System.currentTimeMillis()
      val putObjectRequest = new PutObjectRequest(bucketName, key, file)
      s3ClientCache.getClient(bucketName).putObject(putObjectRequest)
      val objPutEnd = System.currentTimeMillis()
      gauge("object.size", Files.size(file.toPath).toDouble, s"bucketName:$bucketName")
      recordExecutionTime("object.put.time", objPutEnd - objPutStart, s"bucketName:$bucketName")
      logger.info(s"Object $key uploaded to bucket $bucketName successfully.")
    }
  }

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

  override def _moveObject(bucketName: String, fromKey: String, toKey: String): Boolean = {
    try {
      s3ClientCache.getClient(bucketName).copyObject(bucketName, fromKey, bucketName, toKey)
      s3ClientCache.getClient(bucketName).deleteObject(bucketName, fromKey)
      logger.info(s"Object moved from $fromKey to $toKey in bucket $bucketName.")
      true
    } catch {
      case e: Exception =>
        logger.error(s"Error moving object from $fromKey to $toKey in bucket $bucketName", e)
        false
    }
  }

  override def _deleteObjects(bucketName: String, keys: Set[String]): Unit = {
    try {
      val deleteObjectRequest = new DeleteObjectsRequest(bucketName)
        .withKeys(keys.toList: _*)
      val result = s3ClientCache.getClient(bucketName).deleteObjects(deleteObjectRequest)
      logger.info(s"Deleted ${result.getDeletedObjects.size()} objects from bucket $bucketName.")
    } catch {
      case e: Exception =>
        logger.error(s"Error deleting objects from bucket $bucketName with keys $keys", e)
        throw e
    }
  }
}
