package com.cardinal.discovery

trait RouteInput {
  def getTargetId: String = ""
}

case class ClusterState(added: Set[Pod], removed: Set[Pod], current: Set[Pod]) extends RouteInput {
  override def getTargetId: String = ""
}
