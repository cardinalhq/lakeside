package com.cardinal.discovery

case class Pod(ip: String) {
  override def hashCode(): Int = ip.hashCode

  override def equals(obj: Any): Boolean = this.ip == obj.asInstanceOf[Pod].ip
}
