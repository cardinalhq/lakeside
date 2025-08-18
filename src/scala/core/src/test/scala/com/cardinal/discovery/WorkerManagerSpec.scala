package com.cardinal.discovery

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.duration._

class WorkerManagerSpec extends AnyFunSuite with Matchers with Eventually {
  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(100, Millis))

  test("removes pod from ready set when heartbeat terminates") {
    implicit val system: ActorSystem = ActorSystem("worker-manager-spec")
    implicit val mat: Materializer = Materializer(system)
    import system.dispatcher

    val sseSource = Source.tick(0.seconds, 1.second, ByteString("data: ping\n\n"))
    val binding = Await.result(
      Http().newServerAt("127.0.0.1", 7101).bindSync { _ =>
        HttpResponse(entity = HttpEntity(ContentTypes.`text/event-stream`, sseSource))
      },
      3.seconds
    )

    val (queue, src) = Source.queue[ClusterState](16).preMaterialize()
    val scaler = new ClusterScaler { def scaleTo(desiredReplicas: Int): Unit = () }
    val manager = new WorkerManager(src, minWorkers = 0, maxWorkers = 1, scaler)

    val pod = Pod("127.0.0.1")
    queue.offer(ClusterState(Set(pod), Set.empty, Set(pod)))

    eventually { manager.podCount shouldBe 1 }

    Await.result(binding.terminate(1.second), 3.seconds)

    eventually { manager.podCount shouldBe 0 }

    Await.result(system.terminate(), 5.seconds)
  }
}
