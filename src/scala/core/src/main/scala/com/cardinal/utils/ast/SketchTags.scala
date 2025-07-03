package com.cardinal.utils.ast


object SketchTags {
  val HLL_SKETCH_TYPE: String = "hll"
  val DD_SKETCH_TYPE: String = "dd"
  val MAP_SKETCH_TYPE: String = "map"
}
case class SketchTags(tags: Map[String, Any], sketchType: String, sketch: Either[Array[Byte], Map[String, Double]])
