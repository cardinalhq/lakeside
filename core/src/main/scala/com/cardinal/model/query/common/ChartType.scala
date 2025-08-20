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

package com.cardinal.model.query.common

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode, SerializerProvider}

@JsonSerialize(using = classOf[ChartTypeJsonSerializer])
@JsonDeserialize(using = classOf[ChartTypeTypeJsonDeserializer])
trait ChartType {
  def jsonString: String
}

case object COUNT_CHART extends ChartType {
  override def jsonString: String = "count"
}

case object RATE_CHART extends ChartType {
  override def jsonString: String = "rate"
}

object ChartType {

  def fromStr(str: String): ChartType = {
    str.toLowerCase.trim match {
      case "count" => COUNT_CHART
      case "rate"  => RATE_CHART
      case _       => throw new IllegalArgumentException(s"Unknown chart type $str!")
    }
  }
}

class ChartTypeJsonSerializer extends StdSerializer[ChartType](classOf[ChartType]) {
  override def serialize(value: ChartType, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    jgen.writeString(value.jsonString)
  }
}

class ChartTypeTypeJsonDeserializer extends StdDeserializer[ChartType](classOf[ChartType]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): ChartType = {
    val typeStr: String = p.getCodec.readTree[JsonNode](p).textValue()
    ChartType.fromStr(typeStr)
  }
}
