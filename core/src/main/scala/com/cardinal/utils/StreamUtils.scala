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

package com.cardinal.utils

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.cardinal.instrumentation.Metrics.count
import com.netflix.spectator.api.Clock
import com.typesafe.scalalogging.StrictLogging

/**
  * Utility functions for commonly used operations on Akka streams. Most of these are for
  * the purpose of getting additional instrumentation into the behavior of the stream.
  */
object StreamUtils extends StrictLogging {

  /**
    * Return attributes for running a stream with a supervision strategy that will provide
    * some insight into exceptions.
    *
    */
  def supervisionStrategy(): Attributes = {
    ActorAttributes.withSupervisionStrategy { t =>
      count("akka.stream.exceptions", 1.0, s"error:${t.getClass.getSimpleName}")
      logger.warn(s"exception from stream stage", t)
      Supervision.Stop
    }
  }

  /**
    * Creates a queue source based on a `BoundedSourceQueue`. Values offered to the queue
    * will be emitted by the source. Can be used to get metrics when using `Source.queue(size)`
    * that was introduced in [#29770].
    *
    * [#29770]: https://github.com/akka/akka/pull/29770
    *
    * The bounded source queue should be preferred to `Source.queue(*, strategy)`
    * or `Source.actorRef` that can have unbounded memory use if the consumer cannot keep
    * up with the rate of data being offered ([#25798]).
    *
    * [#25798]: https://github.com/akka/akka/issues/25798
    *
    * @param id
    *     Dimension used to distinguish a particular queue usage.
    * @param size
    *     Number of enqueued items to allow before triggering the overflow strategy.
    * @return
    *     Source that emits values offered to the queue.
    */
  def blockingQueue[T](id: String, size: Int): Source[T, SourceQueue[T]] = {
    Source.queue(size).mapMaterializedValue(q => new SourceQueue[T](id, q))
  }

  final class SourceQueue[T](id: String, queue: BoundedSourceQueue[T]) {

    @volatile private var completed: Boolean = false

    /**
      * Add the value into the queue if there is room. Returns true if the value was successfully
      * enqueued.
      */
    def offer(value: T): Boolean = {
      offerWithResult(value) match {
        case QueueOfferResult.Enqueued    => count(id, 1, "op:enqueued"); true
        case QueueOfferResult.Dropped     => count(id, 1, "op:dropped"); false
        case QueueOfferResult.QueueClosed => count(id, 1, "op:closed"); false
        case QueueOfferResult.Failure(e) =>
          logger.error(s"Error in queue.offer for $id", e)
          count(id, 1, "op:failed"); false
      }
    }

    private def offerWithResult(value: T): QueueOfferResult = {
      queue.offer(value)
    }

    /**
      * Indicate that the use of the queue is complete. This will allow the associated stream
      * to finish processing elements and then shutdown. Any new elements offered to the queue
      * will be dropped.
      */
    def complete(): Unit = {
      queue.complete()
      completed = true
    }

    /** Check if the queue is open to take more data. */
    def isOpen: Boolean = !completed

    /** The approximate number of entries in the queue. */
    def size: Int = queue.size()
  }

  /**
    * Filter out repeated values in a stream. Similar to the unix `uniq` command.
    *
    * @param timeout
    *     Repeated value will still be emitted if elapsed time since last emit exceeds
    *     timeout. Unit is milliseconds.
    */
  def unique[V](timeout: Long = Long.MaxValue, clock: Clock = Clock.SYSTEM): Flow[V, V, NotUsed] = {
    Flow[V].via(new UniqueFlow[V](timeout, clock))
  }

  private final class UniqueFlow[V](timeout: Long, clock: Clock) extends GraphStage[FlowShape[V, V]] {

    private val in = Inlet[V]("UniqueFlow.in")
    private val out = Outlet[V]("UniqueFlow.out")

    override val shape: FlowShape[V, V] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

      new GraphStageLogic(shape) with InHandler with OutHandler {
        private var previous: V = _
        private var lastPushedAt: Long = 0

        private def isExpired: Boolean = {
          lastPushedAt == 0 || clock.wallTime() - lastPushedAt > timeout
        }

        override def onPush(): Unit = {
          val v = grab(in)
          if (v == previous && !isExpired) {
            pull(in)
          } else {
            previous = v
            lastPushedAt = clock.wallTime()
            push(out, v)
          }
        }

        override def onPull(): Unit = {
          pull(in)
        }

        setHandlers(in, out, this)
      }
    }
  }
}
