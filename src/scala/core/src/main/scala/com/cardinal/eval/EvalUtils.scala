package com.cardinal.eval

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import com.cardinal.utils.ast.{AST, BaseExpr, EvalResult, SketchInput}

import java.time.Duration

object EvalUtils {

  def astEvalFlow(
    ast: AST,
    baseExprLookup: SketchInput => Option[BaseExpr],
    numBuffers: Int = 4,
    step: Duration
  ): Flow[List[SketchInput], Map[String, EvalResult], NotUsed] = {
    Flow[List[SketchInput]]
      .via(new TimeGroupedSketchAggregator(numBuffers = numBuffers, baseExprLookup))
      .flatMapConcat(Source.apply)
      .map(sketchGroup => ast.eval(sketchGroup, step))
  }
}
