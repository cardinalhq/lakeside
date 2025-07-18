package com.cardinal.utils.ast

import com.cardinal.utils.ast.SketchTags.{DD_SKETCH_TYPE, HLL_SKETCH_TYPE}
import com.datadoghq.sketch.ddsketch.DDSketches
import org.apache.datasketches.hll.{HllSketch, TgtHllType}

trait Aggregator {
  def update(tags: Map[String, Any], value: Double): Unit
  def result(): List[SketchTags]
}

class DDSketchAggregator(tags: Map[String, Any]) extends Aggregator {
  private val ddSketch = DDSketches.unboundedDense(0.01)

  def mergeSketch(value: Double): Unit = {
    ddSketch.accept(value)
  }

  override def update(tags: Map[String, Any], value: Double): Unit = {
    ddSketch.accept(value)
  }

  override def result(): List[SketchTags] =
    List[SketchTags](SketchTags(tags = tags, sketchType = DD_SKETCH_TYPE, sketch = Left(ddSketch.serialize().array())))
}

class HLLAggregator(groupBys: List[String]) extends Aggregator {
  private val hllSketch = new HllSketch(12, TgtHllType.HLL_4)

  def update(v: String): Unit = {
    hllSketch.update(v)
  }

  override def update(tags: Map[String, Any], value: Double): Unit = {
    val value = groupBys.map(g => tags.getOrElse(g, "")).mkString(":")
    update(value)
  }

  override def result(): List[SketchTags] = {
    List[SketchTags](
      SketchTags(tags = Map.empty, sketchType = HLL_SKETCH_TYPE, sketch = Left(hllSketch.toCompactByteArray))
    )
  }
}
