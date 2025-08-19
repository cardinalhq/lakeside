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

package com.cardinal.queryapi

import akka.actor.ActorSystem
import com.cardinal.config.StorageProfileCache
import com.cardinal.queryapi.engine.{QueryEngineV2, SegmentCacheManager, WorkerHeartbeatReceiver}
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
  def segmentCacheManager(actorSystem: ActorSystem, workerHeartbeatReceiver: WorkerHeartbeatReceiver): SegmentCacheManager = {
    implicit val as: ActorSystem = actorSystem
    SegmentCacheManager.setActorSystem(actorSystem)
    SegmentCacheManager.setHeartbeatReceiver(workerHeartbeatReceiver)
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
