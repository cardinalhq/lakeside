package com.cardinal.utils.ast

import com.cardinal.utils.ast.ASTUtils.getFinalGrouping

import java.time.Duration

case class Formula(e1: AST, e2: AST, op: String) extends AST {
  override def toJsonObj: Map[String, AnyRef] = {
    val map = Map.newBuilder[String, AnyRef]
    map += ("e1" -> e1.toJsonObj)
    map += ("e2" -> e2.toJsonObj)
    map += ("op" -> op)
    map.result()
  }

  override def eval(sketchGroup: SketchGroup,  step: Duration): Map[String, EvalResult] = {
    val groupByKeys = getFinalGrouping(this)
    val e1ResultMap = ASTUtils.eval(ast = e1, groupByKeys = groupByKeys, sketchGroup = sketchGroup,  step = step)
    val e2ResultMap = ASTUtils.eval(ast = e2, groupByKeys = groupByKeys, sketchGroup = sketchGroup,  step = step)

    val result = Map.newBuilder[String, EvalResult]
    val allGroupKeys = e1ResultMap.keys ++ e2ResultMap.keys
    allGroupKeys.foreach(groupKeyStr => {
      val e1ResultOpt = e1ResultMap.get(groupKeyStr)
      val e2ResultOpt = e2ResultMap.get(groupKeyStr)
      // If either expressions does not have data for a groupKey set, set the value to 0 with timestamp and tags from the other expression.
      val filledExpressionResults = (e1ResultOpt, e2ResultOpt) match {
        case (Some(_), Some(_)) =>
          (e1ResultOpt, e2ResultOpt)
        case (Some(e1Res), None) if op == "add" => (e1ResultOpt, Some(EvalResult(e1Res.timestamp, 0, e1Res.tags)))
        case (None, Some(e2Res)) if op == "add" => (Some(EvalResult(e2Res.timestamp, 0, e2Res.tags)), e2ResultOpt)
        case _ => (None, None)
      }
      filledExpressionResults match {
        case (Some(e1Result), Some(e2Result)) =>
          op match {
            case "add" =>
              result += groupKeyStr -> EvalResult(e1Result.timestamp, e1Result.value + e2Result.value, e1Result.tags)
            case "sub" =>
              result += groupKeyStr -> EvalResult(e1Result.timestamp, e1Result.value - e2Result.value, e1Result.tags)
            case "mul" =>
              result += groupKeyStr -> EvalResult(e1Result.timestamp, e1Result.value * e2Result.value, e1Result.tags)
            case "div" =>
              if (e2Result.value != 0) {
                result += groupKeyStr -> EvalResult(e1Result.timestamp, e1Result.value / e2Result.value, e1Result.tags)
              }
            // treat divide by zero as MISSING_DATA for downstream processing
          }
        case _ =>
      }
    })
    result.result()
  }

  override def label(tags: Map[String, Any]): String = {
    val e1Label = e1.label(tags)
    val e2Label = e2.label(tags)
    op match {
      case "add" => s"$e1Label + $e2Label"
      case "sub" => s"$e1Label - $e2Label"
      case "mul" => s"$e1Label * $e2Label"
      case "div" => s"$e1Label / $e2Label"
    }
  }
}
