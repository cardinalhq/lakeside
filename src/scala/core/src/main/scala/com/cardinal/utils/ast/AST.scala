package com.cardinal.utils.ast

import java.time.Duration

trait AST {
  def toJsonObj: Map[String, AnyRef]

  def eval(sketchGroup: SketchGroup, step: Duration): Map[String, EvalResult]

  def label(tags: Map[String, Any]): String
}

