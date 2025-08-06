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

