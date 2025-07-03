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
