package com.cardinal.discovery

trait WorkerScaler {
  def scaleTo(desiredReplicas: Int): Unit
}
