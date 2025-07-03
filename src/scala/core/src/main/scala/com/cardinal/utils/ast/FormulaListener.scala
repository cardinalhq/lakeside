package com.cardinal.utils.ast

import com.cardinal.promql.{ArithmeticParserBaseListener, ArithmeticParserLexer, ArithmeticParserParser}
import com.cardinal.utils.ast.ASTUtils.ConstantExpr
import org.antlr.v4.runtime.tree.ParseTreeWalker
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import java.util

class FormulaListener(baseExpressions: Map[String, AST]) extends ArithmeticParserBaseListener {
  // Use a stack to build the AST.
  private val stack: java.util.Stack[AST] = new util.Stack[AST]()

  override def exitExpression(ctx: ArithmeticParserParser.ExpressionContext): Unit = {
    extractOp(ctx) match {
      case Some(op) =>
        // In a binary expression, the right operand is on the top of the stack.
        // Pop it first, then pop the left operand.
        if (stack.size() < 2) {
          throw new IllegalStateException("Not enough operands for operation")
        }
        val right = stack.pop()
        val left = stack.pop()
        val formula = Formula(e1 = left, e2 = right, op = op)
        stack.push(formula)
      case None =>
      // No operator means no action at exit.
    }
  }

  override def enterVariable(ctx: ArithmeticParserParser.VariableContext): Unit = {
    val variableName = ctx.VARIABLE().getText
    baseExpressions.get(variableName) match {
      case Some(ast) => stack.push(ast)
      case None      => throw new IllegalArgumentException(s"Unknown variable '$variableName'")
    }
  }

  override def enterScientific(ctx: ArithmeticParserParser.ScientificContext): Unit = {
    val constant = ConstantExpr(ctx.SCIENTIFIC_NUMBER().getText.toDouble)
    stack.push(constant)
  }

  private def extractOp(ctx: ArithmeticParserParser.ExpressionContext): Option[String] = {
    if (!ctx.PLUS().isEmpty) {
      Some("add")
    } else if (!ctx.MINUS().isEmpty) {
      Some("sub")
    } else if (ctx.TIMES() != null) {
      Some("mul")
    } else if (ctx.DIV() != null) {
      Some("div")
    } else {
      None
    }
  }
}

object FormulaListener {
  private def areBracketsBalanced(expr: String): Boolean = { // Using ArrayDeque is faster than using Stack class
    val stack = new util.ArrayDeque[Char]
    // Traversing the Expression
    for (i <- 0 until expr.length) {
      val x = expr.charAt(i)
      if (x == '(') { // Push the element in the stack
        stack.push(x)
      }
      else {
        // If current character is not opening
        // bracket, then it must be closing. So stack
        // cannot be empty at this point.
        var check: Char = '\u0000'
        x match {
          case ')' =>
            check = stack.pop
            if (check != '(') return false

          case _ =>
        }
      }
    }
    // Check Empty Stack
    stack.isEmpty
  }

  def toFormulaAST(arithmeticExpr: String, baseExpressions: Map[String, AST]): Formula = {
    if(!areBracketsBalanced(arithmeticExpr)) {
      throw new IllegalArgumentException(s"Unbalanced parens in `$arithmeticExpr`")
    }
    val listener = new FormulaListener(baseExpressions)
    val lexer = new ArithmeticParserLexer(CharStreams.fromString(arithmeticExpr))
    val lexerErrorListener = new ErrorListener
    lexer.removeErrorListeners()
    lexer.addErrorListener(lexerErrorListener)
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new ArithmeticParserParser(tokenStream)
    val parserErrorListener = new ErrorListener
    parser.removeErrorListeners()
    parser.addErrorListener(parserErrorListener)
    val walker = new ParseTreeWalker

    walker.walk(listener, parser.expression())
    val lexerError = lexerErrorListener.getError
    val parserError = parserErrorListener.getError
    if (lexerError != null) {
      throw new IllegalArgumentException(s"Invalid formula `$arithmeticExpr` ${lexerError.msg}")
    }
    else if(parserError != null) {
      throw new IllegalArgumentException(s"Invalid formula `$arithmeticExpr` ${parserError.msg}")
    }
    val polled = listener.stack.pop()
    if(polled == null) {
      throw new IllegalArgumentException(s"Invalid formula `$arithmeticExpr`")
    }
    polled.asInstanceOf[Formula]
  }
}
