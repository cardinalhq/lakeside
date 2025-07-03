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
