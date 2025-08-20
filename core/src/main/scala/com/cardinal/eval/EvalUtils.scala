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
