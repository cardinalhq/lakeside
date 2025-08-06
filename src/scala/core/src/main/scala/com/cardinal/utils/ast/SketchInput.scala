/*
 * Copyright (C) 2025 CardinalHQ, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.cardinal.utils.ast

import com.cardinal.discovery.RouteInput
import com.cardinal.utils.ast.SketchInput.DEFAULT_EVAL_STEP
import com.netflix.atlas.json.Json

import java.util.Base64

object SketchInput {
  val DEFAULT_EVAL_STEP: Long = 10000

  def toJson(sketchInput: SketchInput): String = {
    val sketchInputMap = Map.newBuilder[String, AnyRef]
    sketchInputMap += "customerId"   -> sketchInput.customerId
    sketchInputMap += "evalConfigId" -> sketchInput.evalConfigId
    sketchInputMap += "baseExprId"   -> sketchInput.baseExprHashCode
    sketchInputMap += "timestamp"    -> Long.box(sketchInput.timestamp)
    sketchInputMap += "frequency"         -> Long.box(sketchInput.frequency)
    sketchInputMap += "tags"         -> sketchInput.sketchTags.tags
    sketchInput.sketchTags.sketch match {
      case Left(sketch) =>
        sketchInputMap += "bytes" -> Base64.getEncoder.encodeToString(sketch)
        sketchInputMap += "type"  -> "s"
      case Right(map) =>
        sketchInputMap += "map"  -> map
        sketchInputMap += "type" -> "m"
    }
    sketchInputMap += "sketchType" -> sketchInput.sketchTags.sketchType

    Json.encode[Map[String, AnyRef]](sketchInputMap.result())
  }

  def fromJson(json: String): SketchInput = {
    val obj = ujson.read(json).obj
    val customerId = obj("customerId").str
    val evalConfigId = obj("evalConfigId").str
    val baseExprId = obj("baseExprId").str
    val timestamp = obj("timestamp").num.toLong
    val step = obj("frequency").num.toLong
    val tags = obj("tags").obj.map(e => e._1 -> e._2.value).toMap
    val sketch = obj("type").str match {
      case "s" => Left(Base64.getDecoder.decode(obj("bytes").str))
      case "m" => Right(obj("map").obj.map(e => e._1 -> e._2.num).toMap)
    }
    val sketchType = obj("sketchType").str
    SketchInput(
      customerId = customerId,
      evalConfigId = evalConfigId,
      baseExprHashCode = baseExprId,
      timestamp = timestamp,
      frequency = step,
      sketchTags = SketchTags(tags = tags, sketchType = sketchType, sketch = sketch)
    )
  }

}
case class SketchInput(
                        customerId: String,
                        evalConfigId: String = "",
                        baseExprHashCode: String = "",
                        timestamp: Long,
                        frequency: Long = DEFAULT_EVAL_STEP,
                        sketchTags: SketchTags,
                        targetId: String = ""
) extends RouteInput {
  override def getTargetId: String = targetId
}
