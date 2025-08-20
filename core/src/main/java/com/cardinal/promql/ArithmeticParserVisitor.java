// Generated from ArithmeticParser.g4 by ANTLR 4.12.0
package com.cardinal.promql;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link ArithmeticParserParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface ArithmeticParserVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link ArithmeticParserParser#file_}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFile_(ArithmeticParserParser.File_Context ctx);
	/**
	 * Visit a parse tree produced by {@link ArithmeticParserParser#equation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEquation(ArithmeticParserParser.EquationContext ctx);
	/**
	 * Visit a parse tree produced by {@link ArithmeticParserParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(ArithmeticParserParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link ArithmeticParserParser#atom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAtom(ArithmeticParserParser.AtomContext ctx);
	/**
	 * Visit a parse tree produced by {@link ArithmeticParserParser#scientific}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitScientific(ArithmeticParserParser.ScientificContext ctx);
	/**
	 * Visit a parse tree produced by {@link ArithmeticParserParser#variable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVariable(ArithmeticParserParser.VariableContext ctx);
	/**
	 * Visit a parse tree produced by {@link ArithmeticParserParser#relop}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelop(ArithmeticParserParser.RelopContext ctx);
}