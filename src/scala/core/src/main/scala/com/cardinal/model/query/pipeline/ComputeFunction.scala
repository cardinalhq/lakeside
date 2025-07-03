package com.cardinal.model.query.pipeline

import com.cardinal.logs.LogCommons.{NUMBER_TYPE, STRING_TYPE}
import com.netflix.atlas.json.Json
import ujson._

import scala.collection.mutable

sealed trait Argument {
  val `type`: String = "literal"

  def toSql: String

  def eval(tags: Map[String, Any]): Any
}

case class Label(name: String, dataType: String) extends Argument {
  override val `type`: String = "label"

  override def toSql: String = dataType match {
    case NUMBER_TYPE => s"try_cast($name as double)"
    case _           => name
  }

  override def eval(tags: Map[String, Any]): Any = tags.get(name).orNull
}

case class Literal(value: AnyRef, dataType: String) extends Argument {
  override def toSql: String = dataType match {
    case STRING_TYPE => s"'$value'"
    case NUMBER_TYPE => s"$value"
  }

  override def eval(tags: Map[String, Any]): Any = value
}

case class ComputeFunction(
  name: String,
  numArguments: Option[Int],
  returnType: String,
  sqlFunc: List[Argument] => String,
  evalFunc: (List[Argument], Map[String, Any]) => Any,
  arguments: (String, String)*
)

case class FunctionCall(name: String, arguments: List[Argument]) extends Argument {
  override val `type`: String = "functionCall"

  override def toSql: String = ComputeFunction.getSpecs(name).sqlFunc.apply(arguments)

  def toJson: Obj = {
    val functionCallMap = new mutable.LinkedHashMap[String, Value]()
    functionCallMap += "name" -> Str(name)
    val argumentsMap = arguments.map {
      case Label(labelName, dataType) =>
        val labelMap = new mutable.LinkedHashMap[String, Value]()
        labelMap += "name" -> Str(labelName)
        labelMap += "type" -> Str("label")
        labelMap += "dataType" -> Str(dataType)
        Obj(labelMap)

      case Literal(value, dataType) =>
        val literalMap = new mutable.LinkedHashMap[String, Value]()
        literalMap += "type" -> Str("literal")
        dataType match {
          case NUMBER_TYPE => literalMap += "value" -> Num(value.toString.toDouble)
          case _ =>   literalMap += "value" -> value.toString
        }
        literalMap += "dataType" -> Str(dataType)
        Obj(literalMap)

      case f: FunctionCall => f.toJson
    }
    functionCallMap += "arguments" -> Arr(argumentsMap)
    Obj(functionCallMap)
  }

  override def eval(tags: Map[String, Any]): Any = {
    val function = ComputeFunction.registry.get(name)
    function.get.evalFunc.apply(arguments, tags)
  }
}

object ComputeFunction {
  private val LABEL_TYPE: String = "label"
  private val LITERAL_TYPE: String = "literal"
  private val FUNCTION_CALL_TYPE: String = "functionCall"

  private val KNOWN_ARGUMENT_TYPES = Set[String](LABEL_TYPE, LITERAL_TYPE, FUNCTION_CALL_TYPE)

  val registry = new mutable.HashMap[String, ComputeFunction]
  registry += "mul" -> ComputeFunction(
    name = "mul",
    numArguments = None,
    returnType = NUMBER_TYPE,
    sqlFunc = arguments => arguments.map(_.toSql).mkString(" * "),
    evalFunc = (arguments, tags) => arguments.map(arg => arg.eval(tags)).foldLeft(Some(1.0)) {
      case (Some(acc), x: Double) => Some(acc * x)
      case _ => Some(Double.NaN)
    }.getOrElse(Double.NaN),
    arguments = "multiplier" -> NUMBER_TYPE
  )
  registry += "div" -> ComputeFunction(
    name = "div",
    numArguments = Some(2),
    returnType = NUMBER_TYPE,
    sqlFunc = arguments => s"${arguments.head.toSql} / ${arguments.last.toSql}",
    evalFunc = (arguments, tags) => (arguments.head.eval(tags), arguments.last.eval(tags)) match {
      case (x: Double, y: Double) => x / y
      case _ => Double.NaN
    },
    arguments = "numerator" -> NUMBER_TYPE,
    "denominator" -> NUMBER_TYPE
  )

  registry += "add" -> ComputeFunction(
    name = "add",
    numArguments = None,
    returnType = NUMBER_TYPE,
    sqlFunc = arguments => arguments.map(_.toSql).mkString(" + "),
    evalFunc = (arguments, tags) => arguments.map(arg => arg.eval(tags)).foldLeft(Some(1.0)) {
      case (Some(acc), x: Double) => Some(acc + x)
      case _ => Some(Double.NaN)
    }.getOrElse(Double.NaN),
    arguments = "number" -> NUMBER_TYPE
  )

  registry += "sub" -> ComputeFunction(
    name = "sub",
    numArguments = Some(2),
    returnType = NUMBER_TYPE,
    sqlFunc = arguments => s"${arguments.head.toSql} - ${arguments.last.toSql}",
    evalFunc = (arguments, tags) => (arguments.head.eval(tags), arguments.last.eval(tags)) match {
      case (x: Double, y: Double) => x - y
      case _ => Double.NaN
    },
    arguments = "number1" -> NUMBER_TYPE,
    "number2" -> NUMBER_TYPE
  )

  registry += "concat" -> ComputeFunction(
    name = "concat",
    numArguments = None,
    returnType = STRING_TYPE,
    sqlFunc = arguments => s"concat(${arguments.map(_.toSql).mkString(",")})",
    evalFunc = (arguments, tags) => arguments.map(arg => arg.eval(tags)).foldLeft(Some("")) {
      case (Some(acc), x: String) => Some(s"$acc$x")
      case _ => Some("")
    }.getOrElse(Double.NaN),
    arguments = "string" -> STRING_TYPE
  )

  registry += "strpos" -> ComputeFunction(
    name = "strpos",
    numArguments = Some(2),
    returnType = NUMBER_TYPE,
    sqlFunc = arguments => s"position(${arguments.head.toSql} in ${arguments.last.toSql})",
    evalFunc = (arguments, tags) => (arguments.head.eval(tags), arguments.last.eval(tags)) match {
      case (x: String, y: String) => y.indexOf(x)
      case _ => -1
    },
    arguments = "search_string" -> STRING_TYPE,
    "string" -> STRING_TYPE
  )


  def getSpecs: Map[String, ComputeFunction] = registry.toMap

  private def validateFunctionCall(functionCall: FunctionCall): Unit = {
    val funcName = functionCall.name
    val logFunction = registry(funcName)
    val arguments = functionCall.arguments
    logFunction.numArguments match {
      case Some(n) =>
        if (n != arguments.size)
          throw new IllegalArgumentException(
            s"Invalid number of arguments for function $funcName, passed ${arguments.size}, allowed $n"
          )
        val argTypes = logFunction.arguments.iterator
        for (arg <- arguments) {
          val argType = argTypes.next()
          validateArgument(funcName, arg, argType)
        }

      case None => // cardinality is 'n'
        val argType = logFunction.arguments.head
        for (arg <- arguments) {
          validateArgument(funcName, arg, argType)
        }
    }
  }

  private def validateArgument(funcName: String, arg: Argument, argNameType: (String, String)): Unit = {
    arg match {
      case Label(labelName, dataType) =>
        if (argNameType._2 != dataType)
          throw new IllegalArgumentException(s"Invalid dataType $dataType for labelName = $labelName")
      case Literal(value, dataType) =>
        if (argNameType._2 != dataType)
          throw new IllegalArgumentException(s"Invalid dataType $dataType for labelName = $value")
      case f: FunctionCall =>
        val returnType = registry(f.name).returnType
        if (returnType != argNameType._2)
          throw new IllegalArgumentException(
            s"Function ${f.name} returns $returnType, but func $funcName takes ${argNameType._2}"
          )
    }
  }

  def toFunctionCall(json: String): FunctionCall = {
    val obj = ujson.read(json).obj
    if (!obj.contains("name")) {
      throw new IllegalArgumentException("No function name present!")
    }
    val funcName = obj("name").str
    if (!registry.contains(funcName)) {
      throw new IllegalArgumentException(s"Unknown function $funcName!")
    }
    if (!obj.contains("arguments")) {
      throw new IllegalArgumentException(s"No arguments provided for function $funcName")
    }
    val argumentList = obj("arguments").arr.toList
    val argumentBuilder = List.newBuilder[Argument]

    for (arg <- argumentList) {
      if (!arg.obj.contains("type")) {
        throw new IllegalArgumentException(s"Type required for argument!")
      }
      val `type` = arg.obj("type").str
      if (!KNOWN_ARGUMENT_TYPES.contains(`type`)) {
        throw new IllegalArgumentException(
          s"Unknown type: ${`type`}, valid types: ${KNOWN_ARGUMENT_TYPES.mkString(", ")}"
        )
      }

      `type` match {
        case LITERAL_TYPE =>
          val valArg = arg("value")
          valArg.numOpt match {
            case Some(num) => argumentBuilder += Literal(value = Double.box(num), dataType = NUMBER_TYPE)
            case None =>
              valArg.strOpt match {
                case Some(str) => argumentBuilder += Literal(value = str, dataType = STRING_TYPE)
                case None      => throw new IllegalArgumentException(s"Unknown type of value: ${valArg.toString()}")
              }
          }

        case LABEL_TYPE =>
          if (!arg.obj.contains("name")) throw new IllegalArgumentException("No label name specified!")
          val name = arg("name").str
          if (!arg.obj.contains("dataType"))
            throw new IllegalArgumentException(s"No dataType specified for label = $name")
          val dataType = arg("dataType").str
          argumentBuilder += Label(name, dataType)

        case FUNCTION_CALL_TYPE =>
          argumentBuilder += toFunctionCall(arg.toString())
      }
    }
    val functionCall = FunctionCall(name = funcName, arguments = argumentBuilder.result())
    validateFunctionCall(functionCall)
    functionCall
  }
}

object TestLogFunction extends App {

  val json =
    """
      |{
      |  "name": "mul",
      |  "type": "functionCall",
      |  "arguments": [
      |    {
      |      "name": "div",
      |      "type": "functionCall",
      |      "arguments": [
      |        {
      |          "type": "label",
      |          "dataType": "number",
      |          "name": "raw"
      |        },
      |        {
      |          "type": "label",
      |          "dataType": "number",
      |          "name": "compressed"
      |        }
      |      ]
      |    },
      |    {
      |      "type": "literal",
      |      "value": 100
      |    }
      |  ]
      |}
      |""".stripMargin

  println(Json.encode(ComputeFunction.getSpecs))
  println(json)
  val c = ComputeFunction.toFunctionCall(json)
  println(c.toSql)
}
