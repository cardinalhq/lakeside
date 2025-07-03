package com.cardinal.queryapi

import akka.actor.ActorSystem
import com.cardinal.config.StorageProfileCache
import com.cardinal.queryapi.engine.{QueryEngineV2, SegmentCacheManager}
import com.cardinal.utils.Commons.isRunningInKubernetes
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.{Bean, Configuration, DependsOn, Scope}

import scala.util.Try

@Configuration
class QueryApiConfiguration {
  private val logger = LoggerFactory.getLogger(getClass)

  @Bean
  def config(): Config = {
    val appConfig = ConfigFactory.parseResources("application.conf")
    // So we can provide some overrides when running the app locally
    val config = if (!isRunningInKubernetes) {
      Try(ConfigFactory.parseResources("application-laptop.conf")).toOption
        .map(localConf => localConf.withFallback(appConfig))
        .getOrElse(appConfig)
    } else {
      logger.info("Running on kubernetes, using discovery config")
      appConfig
    }

    ConfigFactory.load(config)
  }

  @Bean
  @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
  def segmentCacheManager(actorSystem: ActorSystem): SegmentCacheManager = {
    implicit val as: ActorSystem = actorSystem
    SegmentCacheManager.startDiscovery()
    new SegmentCacheManager(actorSystem = actorSystem)
  }

  @Bean
  @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
  @DependsOn(Array("storageProfileCache"))
  def queryEngineV2(
    actorSystem: ActorSystem,
    config: Config,
    segmentCacheManager: SegmentCacheManager,
    storageProfileCache: StorageProfileCache
  ): QueryEngineV2 = {
    new QueryEngineV2(
      actorSystem = actorSystem,
      config = config,
      segmentCacheManager = segmentCacheManager,
      storageProfileCache = storageProfileCache
    )
  }
}
