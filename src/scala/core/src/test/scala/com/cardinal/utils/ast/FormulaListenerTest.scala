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

import com.cardinal.utils.ast.ASTUtils.{ConstantExpr, toASTInput}
import com.cardinal.utils.ast.SketchTags.MAP_SKETCH_TYPE
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test

import java.time.Duration

class FormulaListenerTest {

  private val payload =
    s"""
       |{
       |  "baseExpressions": {
       |  "a": {
       |    "filter": {
       |      "q1": {
       |        "k": "name",
       |        "v": ["foo"],
       |        "op": "eq"
       |      },
       |      "q2": {
       |        "k": "name",
       |        "v": ["zoo"],
       |        "op": "eq"
       |      },
       |      "op": "and"
       |    },
       |    "groupBys": [],
       |    "aggregation": "sum",
       |    "type": "base_expr"
       |  },
       |  "b": {
       |    "filter": {
       |      "k": "name",
       |      "v": ["bar"],
       |      "op": "eq"
       |    },
       |    "groupBys": [],
       |    "chart": {
       |      "aggregation": "sum"
       |    }
       |  }
       |  },
       |  "formulae": {
       |    "f": "sample_formula"
       |  }
       |}
       |""".stripMargin

  @Test
  def testComplexFormula(): Unit = {
    val formula = toFormula("(a - b) * (a + b)")
    assert(formula.op == "mul")
    assert(formula.e1.isInstanceOf[Formula])
    assert(formula.e1.asInstanceOf[Formula].op == "sub")
    assert(formula.e2.isInstanceOf[Formula])
    assert(formula.e2.asInstanceOf[Formula].op == "add")
  }

  @Test
  def testAddZero(): Unit = {
    val formula = toFormula("((a + 0) / b) * 100")
    assert(formula.op == "mul")
    val formula2 = formula.e1.asInstanceOf[Formula]
    val baseExprB = formula2.e2.asInstanceOf[BaseExpr]

    val sg1 = SketchGroup(
      1L,
      Map[BaseExpr, List[SketchInput]](
        baseExprB -> List(
          SketchInput(
            customerId = "customerId1",
            timestamp = 1L,
            sketchTags = SketchTags(
              tags = Map("name" -> "foo"),
              sketchType = MAP_SKETCH_TYPE,
              sketch = Right(Map[String, Double]("sum" -> 1.0))
            )
          )
        )
      )
    )
    val result = formula.eval(sg1, Duration.ofSeconds(10))
    assert(result.size == 1)
  }

  @Test
  def testComplexFormulaWithConstants(): Unit = {
    val formula = toFormula("((a - b) / (a + b)) * 100")
    assert(formula.op == "mul")
    assert(formula.e2.isInstanceOf[ConstantExpr])
    assert(formula.e2.asInstanceOf[ConstantExpr].value == 100.0)
    assert(formula.e1.isInstanceOf[Formula])
    assert(formula.e1.asInstanceOf[Formula].op == "div")
  }

  @Test
  def testInvalidExprMissingRightParenShouldThrowValidation(): Unit = {
    try {
      val _ = toFormula("(a * b")
      fail("Validation was not thrown for invalid expression!")
    } catch {
      case _: Exception =>
    }
  }

  private def toFormula(expr: String): Formula = {
    val astInput = toASTInput(payload.replace("sample_formula", expr))
    FormulaListener.toFormulaAST(astInput.formulae.head, astInput.baseExpressions)
  }
}
