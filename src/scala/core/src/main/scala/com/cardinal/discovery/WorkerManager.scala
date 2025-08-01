package com.cardinal.discovery

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.ExecutionContext

class WorkerManager(
                     watcher: Source[ClusterState, NotUsed],
                     minWorkers: Int,
                     maxWorkers: Int,
                     scaler: WorkerScaler
                   )(implicit system: ActorSystem, mat: Materializer) {

  private implicit val ec: ExecutionContext = system.dispatcher

  private val currentPods = new AtomicReference[Set[Pod]](Set.empty)
  private val slotMap   = mutable.Map.empty[Pod, Int]
  private val freeSlots = mutable.TreeSet.empty[Int]

  watcher.runForeach { state =>
    state.removed.foreach { pod =>
      slotMap.remove(pod).foreach(freeSlots += _)
    }

    state.added.toSeq.sortBy(_.ip).foreach { pod =>
      val slot = if (freeSlots.nonEmpty) freeSlots.min
      else slotMap.size + freeSlots.size
      freeSlots  -= slot
      slotMap(pod) = slot
    }

    currentPods.set(state.current)

    val sz = state.current.size
    if (sz < minWorkers)       scaler.scaleTo(minWorkers)
    else if (sz > maxWorkers)  scaler.scaleTo(maxWorkers)
  }

  /**
   * Pick the same Pod for each key by hashing and modulo over stable slots.
   */
  def getWorkerFor(key: String): Option[Pod] = {
    val podsBySlot = slotMap.toSeq.sortBy(_._2).map(_._1)
    if (podsBySlot.isEmpty) None
    else {
      val idx = (key.hashCode.abs % podsBySlot.size)
      Some(podsBySlot(idx))
    }
  }
}
