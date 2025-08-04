package com.cardinal.queryapi

import akka.actor.ActorSystem
import com.cardinal.config.StorageProfileCache
import com.cardinal.queryapi.engine.{QueryEngineV2, SegmentCacheManager}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.{Bean, Configuration, DependsOn, Scope}

@Configuration
class QueryApiConfiguration {
  private val logger = LoggerFactory.getLogger(getClass)

  @Bean
  def config(): Config = {
    val config = ConfigFactory.parseResources("application.conf")
    ConfigFactory.load(config)
  }

  @Bean
  @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
  def segmentCacheManager(actorSystem: ActorSystem): SegmentCacheManager = {
    implicit val as: ActorSystem = actorSystem
    new SegmentCacheManager()
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
