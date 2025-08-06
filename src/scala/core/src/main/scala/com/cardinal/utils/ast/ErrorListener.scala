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

import org.antlr.v4.runtime.{BaseErrorListener, RecognitionException, Recognizer}

case class Error(value: Any, msg: String, i: Int, i1: Int)

class ErrorListener extends BaseErrorListener {
  private var error: Error = _

  override def syntaxError(
    recognizer: Recognizer[_, _],
    offendingSymbol: Any,
    line: Int,
    charPositionInLine: Int,
    msg: String,
    e: RecognitionException
  ): Unit = {
    val errorMsg = "line " + line + ":" + charPositionInLine + " at " + offendingSymbol + ": " + msg
    this.error = Error(offendingSymbol, errorMsg, line, charPositionInLine)
  }

  def getError: Error = error
}
