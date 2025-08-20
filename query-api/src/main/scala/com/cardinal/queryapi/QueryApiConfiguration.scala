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
import akka.stream.Materializer
import com.cardinal.config.StorageProfileCache
import com.cardinal.discovery.{ClusterScaler, ClusterWatcher, WorkerManager}
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
  @DependsOn(Array("workerManager"))
  def segmentCacheManager(actorSystem: ActorSystem, workerHeartbeatReceiver: WorkerHeartbeatReceiver, workerManager: WorkerManager): SegmentCacheManager = {
    implicit val as: ActorSystem = actorSystem
    SegmentCacheManager.setActorSystem(actorSystem)
    SegmentCacheManager.setHeartbeatReceiver(workerHeartbeatReceiver)
    SegmentCacheManager.setWorkerManager(workerManager)
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

  @Bean
  @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
  def workerManager(actorSystem: ActorSystem, workerHeartbeatReceiver: WorkerHeartbeatReceiver): WorkerManager = {
    implicit val as: ActorSystem = actorSystem
    implicit val mat: Materializer = Materializer(actorSystem)
    
    val minWorkers = sys.env.getOrElse("NUM_MIN_QUERY_WORKERS", "2").toInt
    val maxWorkers = sys.env.getOrElse("NUM_MAX_QUERY_WORKERS", "30").toInt
    
    new WorkerManager(
      watcher = ClusterWatcher.watch(),
      minWorkers = minWorkers,
      maxWorkers = maxWorkers,
      scaler = ClusterScaler.load(),
      getHeartbeatingWorkers = () => workerHeartbeatReceiver.getReadyWorkerCount,
      getHeartbeatingWorkerFor = (key: String) => workerHeartbeatReceiver.getWorkerFor(key),
      isLegacyMode = false
    )
  }
}
