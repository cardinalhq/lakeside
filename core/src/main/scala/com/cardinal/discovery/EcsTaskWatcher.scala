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

package com.cardinal.discovery

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{BroadcastHub, Keep, Source}
import software.amazon.awssdk.services.ecs.EcsClient
import software.amazon.awssdk.services.ecs.model.{DescribeTasksRequest, ListTasksRequest, Task}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object EcsTaskWatcher {

  /**
    * Watch the given ECS service in `clusterName`, emitting ClusterState diffs
    * whenever the set of task IPs changes.
    */
  def startWatching(
    serviceName: String,
    clusterName: String,
    pollingInterval: FiniteDuration = 10.seconds
  )(
    implicit
    system: ActorSystem,
    mat: Materializer
  ): Source[ClusterState, NotUsed] = {
    implicit val ec: ExecutionContext = system.dispatcher

    val ecs: EcsClient = EcsClient.builder().build()

    val currentPods = new AtomicReference[Set[Pod]](Set.empty)

    val (queue, source) = Source
      .queue[ClusterState](16, OverflowStrategy.dropHead)
      .toMat(BroadcastHub.sink)(Keep.both)
      .run()

    def extractPods(tasks: Seq[Task]): Set[Pod] = {
      tasks.flatMap { task =>
        task.attachments.asScala
          .find(_.`type`() == "ElasticNetworkInterface")
          .flatMap { att =>
            att.details.asScala.find(_.name() == "privateIPv4Address").map(_.value())
          }
          .map { ip =>
            Pod(ip)
          }
      }.toSet
    }

    def rebuildPodSet(): Set[Pod] = {
      val listReq = ListTasksRequest
        .builder()
        .cluster(clusterName)
        .serviceName(serviceName)
        .build()

      val taskArns = ecs.listTasks(listReq).taskArns().asScala.toList
      if (taskArns.isEmpty) {
        Set.empty
      } else {
        val descReq = DescribeTasksRequest
          .builder()
          .cluster(clusterName)
          .tasks(taskArns.asJava)
          .build()
        val tasks = ecs.describeTasks(descReq).tasks().asScala.toSeq
        extractPods(tasks)
      }
    }

    def pollOnce(): Unit = {
      val newSet = rebuildPodSet()
      val oldSet = currentPods.getAndSet(newSet)
      if (newSet != oldSet) {
        val state = ClusterState(
          added = newSet.diff(oldSet),
          removed = oldSet.diff(newSet),
          current = newSet
        )
        queue.offer(state)
      }
    }

    pollOnce()
    system.scheduler.scheduleAtFixedRate(pollingInterval, pollingInterval)(() => pollOnce())

    source
  }
}
