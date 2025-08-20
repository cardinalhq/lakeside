// Generated from ArithmeticParser.g4 by ANTLR 4.12.0
package com.cardinal.promql;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link ArithmeticParserParser}.
 */
public interface ArithmeticParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link ArithmeticParserParser#file_}.
	 * @param ctx the parse tree
	 */
	void enterFile_(ArithmeticParserParser.File_Context ctx);
	/**
	 * Exit a parse tree produced by {@link ArithmeticParserParser#file_}.
	 * @param ctx the parse tree
	 */
	void exitFile_(ArithmeticParserParser.File_Context ctx);
	/**
	 * Enter a parse tree produced by {@link ArithmeticParserParser#equation}.
	 * @param ctx the parse tree
	 */
	void enterEquation(ArithmeticParserParser.EquationContext ctx);
	/**
	 * Exit a parse tree produced by {@link ArithmeticParserParser#equation}.
	 * @param ctx the parse tree
	 */
	void exitEquation(ArithmeticParserParser.EquationContext ctx);
	/**
	 * Enter a parse tree produced by {@link ArithmeticParserParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(ArithmeticParserParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link ArithmeticParserParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(ArithmeticParserParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link ArithmeticParserParser#atom}.
	 * @param ctx the parse tree
	 */
	void enterAtom(ArithmeticParserParser.AtomContext ctx);
	/**
	 * Exit a parse tree produced by {@link ArithmeticParserParser#atom}.
	 * @param ctx the parse tree
	 */
	void exitAtom(ArithmeticParserParser.AtomContext ctx);
	/**
	 * Enter a parse tree produced by {@link ArithmeticParserParser#scientific}.
	 * @param ctx the parse tree
	 */
	void enterScientific(ArithmeticParserParser.ScientificContext ctx);
	/**
	 * Exit a parse tree produced by {@link ArithmeticParserParser#scientific}.
	 * @param ctx the parse tree
	 */
	void exitScientific(ArithmeticParserParser.ScientificContext ctx);
	/**
	 * Enter a parse tree produced by {@link ArithmeticParserParser#variable}.
	 * @param ctx the parse tree
	 */
	void enterVariable(ArithmeticParserParser.VariableContext ctx);
	/**
	 * Exit a parse tree produced by {@link ArithmeticParserParser#variable}.
	 * @param ctx the parse tree
	 */
	void exitVariable(ArithmeticParserParser.VariableContext ctx);
	/**
	 * Enter a parse tree produced by {@link ArithmeticParserParser#relop}.
	 * @param ctx the parse tree
	 */
	void enterRelop(ArithmeticParserParser.RelopContext ctx);
	/**
	 * Exit a parse tree produced by {@link ArithmeticParserParser#relop}.
	 * @param ctx the parse tree
	 */
	void exitRelop(ArithmeticParserParser.RelopContext ctx);
}