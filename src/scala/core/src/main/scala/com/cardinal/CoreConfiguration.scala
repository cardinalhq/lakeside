package com.cardinal

import akka.actor.ActorSystem
import com.amazonaws.services.securitytoken.{AWSSecurityTokenService, AWSSecurityTokenServiceClientBuilder}
import com.cardinal.auth.AwsCredentialsCache
import com.cardinal.config.{StorageProfileCache, StorageProfileCacheControlPlane, StorageProfileCacheFile}
import com.cardinal.core.Environment
import com.cardinal.objectstorage._
import org.slf4j.LoggerFactory
import org.springframework.cache.CacheManager
import org.springframework.cache.caffeine.CaffeineCacheManager
import org.springframework.context.annotation._

@Configuration
@ComponentScan(basePackages = Array("com.cardinal.auth", "com.cardinal.utils", "com.cardinal.objectstorage"))
class CoreConfiguration {

  private val logger = LoggerFactory.getLogger(getClass)

  @Bean()
  def env(): Environment = {
    logger.info("Current environment: {}", Environment.getCurrentRegion)
    new Environment
  }

  @Bean(value = Array("storageProfileCache"))
  def storageProfileCache(actorSystem: ActorSystem): StorageProfileCache = {
    // if STORAGE_PROFILE_FILE is set, use that to initialize the cache
    val storageProfileFile = sys.env.get("STORAGE_PROFILE_FILE")
    if (storageProfileFile.isDefined) {
      logger.info("Using storage profile file: {}", storageProfileFile.get)
      return StorageProfileCacheFile.fromFile(storageProfileFile.get)
    }
    val storageProfileCache  : StorageProfileCache = new StorageProfileCacheControlPlane(actorSystem)
    storageProfileCache.asInstanceOf[StorageProfileCacheControlPlane].start()
    storageProfileCache
  }

  @Bean
  @Primary
  def credentialCacheManager(): CacheManager = {
    val cacheManager = new CaffeineCacheManager()
    cacheManager.setCacheSpecification("expireAfterWrite=30m")
    cacheManager
  }

  @Bean
  def clientCacheManager(): CacheManager = {
    val cacheManager = new CaffeineCacheManager()
    cacheManager.setCacheSpecification("expireAfterWrite=5m")
    cacheManager
  }

  @Bean
  @Profile(Array("local","aws"))
  def stsClient(): AWSSecurityTokenService = {
    AWSSecurityTokenServiceClientBuilder.defaultClient()
  }

  @DependsOn(Array("storageProfileCache"))
  @Profile(Array("local", "aws"))
  @Bean
  def awsCredentialsCache(stsClient: AWSSecurityTokenService): AwsCredentialsCache = new AwsCredentialsCache(stsClient)

  @Profile(Array("aws"))
  @Bean
  def objectStoreAws(s3ClientCache: S3ClientCache): ObjectStore =
    new S3Store(s3ClientCache = s3ClientCache)

  @Profile(Array("local"))
  @Bean
  def objectStoreLocal(): ObjectStore =
    new LocalObjectStore(sys.env.getOrElse("LOCAL_BUCKET_ROOT_PATH", throw new NoSuchElementException("LOCAL_BUCKET_ROOT_PATH environment variable is required for local object store testing.  Point this to the root of where you want to test.")))
}
