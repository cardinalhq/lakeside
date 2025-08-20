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

import com.cardinal.utils.Commons.{METRIC_TYPE_COUNTER, METRIC_TYPE_GAUGE, METRIC_TYPE_HISTOGRAM, METRIC_TYPE_RATE}
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode, SerializerProvider}
import com.netflix.atlas.json.Json

@JsonSerialize(using = classOf[MetricTypeJsonSerializer])
@JsonDeserialize(using = classOf[MetricTypeJsonDeserializer])
trait MetricType {
  def jsonString: String
}

case object RATE extends MetricType {
  override def jsonString: String = METRIC_TYPE_RATE
}

case object COUNTER extends MetricType {
  override def jsonString: String = METRIC_TYPE_COUNTER
}

case object GAUGE extends MetricType {
  override def jsonString: String = METRIC_TYPE_GAUGE
}


case object HISTOGRAM extends MetricType {
  override def jsonString: String = METRIC_TYPE_HISTOGRAM
}

class MetricTypeJsonSerializer extends StdSerializer[MetricType](classOf[MetricType]) {
  override def serialize(value: MetricType, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    jgen.writeString(value.toString)
  }
}

class MetricTypeJsonDeserializer extends StdDeserializer[MetricType](classOf[MetricType]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): MetricType = {
    val typeStr: String = p.getCodec.readTree[JsonNode](p).textValue()
    MetricType.fromStr(typeStr)
  }
}

object MetricType {

  def fromStr(str: String): MetricType = {
    str.toLowerCase.trim match {
      case METRIC_TYPE_RATE    => RATE
      case METRIC_TYPE_COUNTER  | "counter" => COUNTER
      case METRIC_TYPE_GAUGE   => GAUGE
      case METRIC_TYPE_HISTOGRAM => HISTOGRAM
      case _                   => throw new IllegalArgumentException(s"Unknown metric type $str!")
    }
  }
}

object TestCustomSerde extends App {
  case class MetricTypeHolder(metricType: MetricType)
  private val holder = MetricTypeHolder(metricType = GAUGE)
  val jsonString = Json.encode[MetricTypeHolder](holder)
  println(jsonString)
  private val deserializedHolder = Json.decode[MetricTypeHolder](jsonString)
  println(deserializedHolder)
}
