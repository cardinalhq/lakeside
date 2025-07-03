package com.cardinal.objectstorage

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.cardinal.auth.AwsCredentialsCache
import com.cardinal.config.StorageProfileCache
import org.slf4j.LoggerFactory
import org.springframework.cache.annotation.{CacheConfig, Cacheable}
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Profile(Array("local","aws"))
@Component
@CacheConfig(cacheNames = Array("clients"),cacheManager = "clientCacheManager")
class S3ClientCache(awsCredentialsCache: AwsCredentialsCache, storageProfileCache: StorageProfileCache) {
  private val logger = LoggerFactory.getLogger(getClass)

  @Cacheable(value=Array("clients"), key = "#bucket" , sync = true)
  def getClient(bucket: String): AmazonS3 = {

    try {
      val optionStorageProfile = storageProfileCache.getStorageProfile(bucket)
      optionStorageProfile match {
        case Some(storageProfile) if storageProfile.role != null =>
          logger.debug(s"Found storage profile ${storageProfile.storageProfileId} for bucket $bucket in region ${storageProfile.region} with role ${storageProfile.role}")
          val newClient = createS3Client(
            storageProfile.region,
            storageProfile.role,
            storageProfile.organizationId,
            storageProfile.endpoint,
          )
          newClient
        case _ =>
          logger.debug(s"No storage profile for bucket $bucket, creating default client")
          throw new RuntimeException(s"Could not create client because no storage profile found for bucket! $bucket")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error creating S3 client for bucket $bucket", e)
        throw e
    }

  }


  private def createS3Client(
                              region: String,
                              roleArn: String,
                              externalId: String,
                              endpoint : Option[String],
                            ): AmazonS3 = {
    try {
      val clientConfig = new ClientConfiguration()
        .withMaxConnections(200)                     // Bump up if you're highly concurrent
        .withConnectionTimeout(10_000)               // 10 sec
        .withSocketTimeout(10_000)
        .withMaxErrorRetry(5)                        // Retrying transient errors
        .withRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(5))

      AmazonS3ClientBuilder
        .standard()
        .withClientConfiguration(clientConfig)
        .withCredentials(new AWSStaticCredentialsProvider(awsCredentialsCache.getCredentials(roleArn, externalId)))
        .withPathStyleAccessEnabled(true)
        .withEndpointConfiguration(
            endpoint match {
                case Some(ep) => new EndpointConfiguration(ep, region)
                case None => new EndpointConfiguration(s"s3.$region.amazonaws.com", region)
            }
        )
        .build()
    } catch {
      case e: Exception =>
        logger.error(s"Error creating S3 client with region $region and role $roleArn", e)
        throw e
    }
  }
}
