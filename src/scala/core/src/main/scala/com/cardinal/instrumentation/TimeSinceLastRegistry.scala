package com.cardinal.instrumentation

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.cardinal.instrumentation.Metrics.gauge
import org.springframework.stereotype.Component

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.PostConstruct
import javax.inject.Inject
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.SetHasAsScala

@Component
class TimeSinceLastRegistry @Inject()(actorSystem: ActorSystem) {
  implicit val as: ActorSystem = actorSystem
  implicit val ec: ExecutionContext = actorSystem.dispatcher

  private val longs = new ConcurrentHashMap[String, AtomicLong]()

  private def currentTime: Long = System.currentTimeMillis()

  @PostConstruct
  def startPolling(): Unit = {
    Source
      .tick(5.seconds, 5.seconds, NotUsed)
      .flatMapConcat(_ => Source(longs.entrySet().asScala.toSet))
      .mapAsync(5) { entry =>
        Future {
          val long = entry.getValue
          gauge(entry.getKey, (currentTime - long.get()).toDouble)
        }
      }
      .toMat(Sink.ignore)(Keep.right)
      .run()
  }
}
