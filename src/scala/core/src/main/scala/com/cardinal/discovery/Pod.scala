package com.cardinal.discovery

case class Pod(ip: String, slotId: Int) {
  override def hashCode(): Int = ip.hashCode

  override def equals(obj: Any): Boolean = this.ip == obj.asInstanceOf[Pod].ip

  def lastTwoIpSegments: Int = {
    val split = ip.split("\\.")
    (split(2) + split(3)).toInt
  }
}
