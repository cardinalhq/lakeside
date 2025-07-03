package com.cardinal.utils.transport

import com.cardinal.discovery.Pod
import com.cardinal.utils.Commons
import org.slf4j.LoggerFactory

import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap, ThreadLocalRandom}

class HashRing {
  private val logger = LoggerFactory.getLogger(getClass)
  private val ring = new ConcurrentSkipListMap[Long, Pod]
  private val hashesByIp = new ConcurrentHashMap[String, Long]()

  def add(pod: Pod): Unit = synchronized {
    val podHash = ThreadLocalRandom.current().nextLong(Long.MinValue, Long.MaxValue)
    ring.put(podHash, pod)
    hashesByIp.put(pod.ip, podHash)
    logger.info(s"Added pod ${pod.ip}/${pod.slotId}")
  }

  def isEmpty: Boolean = ring.isEmpty

  def remove(pod: Pod): Unit = synchronized {
    val hash = Option(hashesByIp.remove(pod.ip))
    if (hash.nonEmpty) {
      val removed = ring.remove(hash.get)
      logger.info(s"Removing pod ${pod.ip}/${pod.slotId}, removed = $removed")
    }
  }

  def nonEmpty: Boolean = {
    !ring.isEmpty
  }

  def getTargetPod(targetId: String): Pod = {
    try {
      if (ring.isEmpty) {
        return null
      }
      var hash = if (targetId.isEmpty) {
        // random partitioning
        ThreadLocalRandom.current().nextLong(Long.MinValue, Long.MaxValue)
      } else {
        Commons.computeHash(targetId)
      }

      if (!ring.containsKey(hash)) {
        val tailMap = ring.tailMap(hash)
        hash = if (tailMap.isEmpty) {
          ring.firstKey
        } else {
          tailMap.firstKey
        }
      }
      ring.get(hash)
    } catch {
      case e: Exception =>
        logger.error("Error in target pod calculation", e)
        null
    }
  }
}
