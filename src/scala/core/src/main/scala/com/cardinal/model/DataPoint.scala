package com.cardinal.model

case class DataPoint(timestamp: Long, value: Double, tags: Map[String, String])
