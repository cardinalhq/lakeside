package com.cardinal.model

import akka.http.scaladsl.model.HttpEntity.ChunkStreamPart
import com.fasterxml.jackson.annotation.JsonIgnore
import com.netflix.atlas.json.JsonSupport

sealed trait SSEMessage extends JsonSupport {
  @JsonIgnore
  protected val TwoLineSeparator = "\r\n\r\n"

  def toChunkStreamPart: ChunkStreamPart = ChunkStreamPart("data: " + toJson + TwoLineSeparator)
}

case class Done(`type`: String = "done") extends SSEMessage

case class Heartbeat(`type`: String = "heartbeat") extends SSEMessage

case class GenericSSEPayload(id: String = "_", `type`: String = "data", message: Map[String, AnyRef]) extends SSEMessage
case class GenericSSEStringPayload(id: String = "_", `type`: String = "data", message: String) extends SSEMessage {
  override def toChunkStreamPart: ChunkStreamPart = ChunkStreamPart("data: " + s"{\"message\": $message}" + TwoLineSeparator)
}

