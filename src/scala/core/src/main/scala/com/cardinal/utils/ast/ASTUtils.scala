package com.cardinal.utils.ast

import com.cardinal.logs.LogCommons._
import com.cardinal.model.query.common._
import com.cardinal.model.query.pipeline.{Compute, ComputeFunction, ExtractedField, Extractor}
import com.cardinal.utils.Commons._
import com.cardinal.utils.ast.BaseExpr.CARDINALITY_ESTIMATE_AGGREGATION
import com.cardinal.utils.ast.queries._
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode, ObjectMapper, SerializerProvider}
import com.netflix.atlas.json.Json
import com.netflix.spectator.atlas.impl.Query
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala

object ASTUtils {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def eval(
    ast: AST,
    groupByKeys: Set[String],
    sketchGroup: SketchGroup,
    step: Duration
  ): Map[String, EvalResult] = {
    ast match {
      case b: BaseExpr => b.eval(sketchGroup, step = step)
      case ConstantExpr(value) =>
        if (groupByKeys.isEmpty) {
          Map[String, EvalResult](
            "default" -> EvalResult(timestamp = sketchGroup.timestamp, value = value, tags = Map())
          )
        } else {
          val sketchInputs = sketchGroup.group.values.toList.flatten
          sketchInputs.map { sketchInput =>
            val tags = sketchInput.sketchTags.tags
            toGroupByKey(groupByKeys, tags) -> EvalResult(
              timestamp = sketchGroup.timestamp,
              value = value,
              tags = tags
            )
          }.toMap
        }
      case f: Formula => f.eval(sketchGroup, step = step)
      case _          => Map.empty
    }
  }

  def getBaseExpressionsUsedInAST(ast: AST): List[BaseExpr] = {
    ast match {
      case b: BaseExpr        => List(b)
      case Formula(e1, e2, _) => getBaseExpressionsUsedInAST(e1) ++ getBaseExpressionsUsedInAST(e2)
      case _                  => List()
    }
  }

  def getFinalGrouping(ast: AST): Set[String] = {
    ast match {
      case BaseExpr(_, _, _, _, _, chartOpts, _, _, _, _) => chartOpts.map(_.groupBys.toSet).getOrElse(Set())
      case Formula(e1, e2, _)                             => getFinalGrouping(e1) ++ getFinalGrouping(e2)
      case _                                              => Set()
    }
  }

  def toGroupByKey(groupByKeys: Set[String], tags: Map[String, Any]): String = {
    groupByKeys.toList.sorted.map(key => tags.getOrElse(key, "").toString).mkString(":")
  }

  private def populateFieldsUsedInFilter(filter: QueryClause, metadata: mutable.HashMap[String, (Boolean, Boolean)]): Unit = {
    filter match {
      case f: Filter => metadata.put(f.k, (f.extracted, f.computed))
      case BinaryClause(q1, q2, _) =>
        populateFieldsUsedInFilter(q1, metadata)
        populateFieldsUsedInFilter(q2, metadata)
      case NotClause(not) =>
        populateFieldsUsedInFilter(not, metadata)
    }
  }

  sealed trait QueryClause {
    override def toString: String = {
      this match {
        case Filter(k, v, op, _, _, _) =>
          op match {
            case EQ       => s"$k = ${v.head}"
            case GT       => s"$k > ${v.head}"
            case GE       => s"$k >= ${v.head}"
            case LT       => s"$k < ${v.head}"
            case LE       => s"$k <= ${v.head}"
            case REGEX    => s"regexMatches($k, ${v.head})"
            case CONTAINS => s"$k contains ${v.head}"
            case IN       => s"$k in (${v.mkString(", ")})"
            case _        => ""
          }
        case BinaryClause(q1, q2, op) =>
          s"(${q1.toString} $op ${q2.toString})"
        case NotClause(not) => s"not(${not.toString})"
      }
    }
  }

  case class Filter(
    k: String,
    v: List[String] = List(),
    op: String,
    extracted: Boolean,
    computed: Boolean,
    dataType: String = STRING_TYPE
  ) extends QueryClause {
    def labelName: String = k
  }

  case class BinaryClause(q1: QueryClause, q2: QueryClause, op: String) extends QueryClause

  case class NotClause(not: QueryClause) extends QueryClause

  @JsonSerialize(using = classOf[ASTInputJsonSerializer])
  @JsonDeserialize(using = classOf[ASTInputJsonDeserializer])
  case class ASTInput(baseExpressions: Map[String, BaseExpr], formulae: List[String] = List()) {

    def toJsonObj: Map[String, AnyRef] = {
      Map(
        "baseExpressions" -> baseExpressions.map(baseExprMap => { baseExprMap._1 -> baseExprMap._2.toJsonObj }),
        "formulae"        -> formulae
      )
    }

    def withFilter(filterToAdd: Filter): ASTInput = {
      this.copy(
        baseExpressions = baseExpressions.map(
          baseExpr => baseExpr._1 -> baseExpr._2.copy(filter = BinaryClause(baseExpr._2.filter, filterToAdd, "and"))
        )
      )
    }
  }

  private class ASTInputJsonSerializer extends StdSerializer[ASTInput](classOf[ASTInput]) {
    override def serialize(value: ASTInput, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
      jgen.writeObject(value.toJsonObj)
    }
  }

  private class ASTInputJsonDeserializer extends StdDeserializer[ASTInput](classOf[ASTInput]) {
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): ASTInput = {
      val jsonNode = p.getCodec.readTree[JsonNode](p)
      val iterator = jsonNode.fields().asScala
      val baseExprNodes = Map.newBuilder[String, BaseExpr]
      var formulae = List[String]()
      while (iterator.hasNext) {
        val node = iterator.next()
        node.getKey match {
          case "formulae" =>
            formulae = node.getValue.elements().asScala.map(_.textValue()).toList
          case "baseExpressions" =>
            node.getValue
              .fields()
              .asScala
              .foreach(mapEntry => {
                baseExprNodes += (mapEntry.getKey -> toBaseExpr(mapEntry.getKey, mapEntry.getValue))
              })
        }
      }
      ASTInput(baseExpressions = baseExprNodes.result(), formulae = formulae)
    }
  }

  // Normalize the computed value from global aggregation, if inverse chart and metric type combinations are used.
  def getTransformerFunc(
    chartType: ChartType,
    metricType: MetricType,
    dataset: String,
    stepInMillis: Long
  ): Double => Double = {
    dataset match {
      case METRICS =>
        (chartType, metricType) match {
          case (COUNT_CHART, RATE) =>
            (v: Double) =>
              v * (stepInMillis / 1000) // rate value * number of seconds in a step computes count
          case (RATE_CHART, COUNTER) =>
            (v: Double) =>
              v / (stepInMillis / 1000) // count value / number of seconds in a step computes rate
          case _ =>
            (d: Double) =>
              d
        }
      case _ =>
        chartType match {
          case RATE_CHART =>
            (v: Double) =>
              v / (stepInMillis / 1000)
          case _ =>
            (d: Double) =>
              d
        }
    }
  }

  // Charting options for logs dataset
  case class ChartOptions(
    @JsonProperty("aggregation") aggregation: String,
    @JsonProperty("groupBys") groupBys: List[String],
    @JsonProperty("type") `type`: ChartType = COUNT_CHART,
    @JsonProperty("rollup") rollupAggregation: Option[String] = None,
    @JsonProperty("fieldName") fieldName: Option[String] = None,
    @JsonProperty("fieldType") fieldType: Option[String] = None
  ) {

    def rollupAggregation(dataSetType: String): Option[String] = {
      dataSetType match {
        case LOGS | SPANS => None
        case _ =>
          rollupAggregation.get match {
            case r =>
              if (r.startsWith("p") || aggregation.startsWith("p") || aggregation == CARDINALITY_ESTIMATE_AGGREGATION)
                None
              else Some(r)
          }
      }
    }

    def toJsonObj: Map[String, AnyRef] = {
      val map = Map.newBuilder[String, AnyRef]
      map += ("aggregation" -> aggregation)
      map += ("groupBys"    -> groupBys)
      map += ("type"        -> `type`.jsonString)
      if (rollupAggregation.isDefined) {
        map += ("rollup" -> rollupAggregation)
      }
      if (fieldName.isDefined) {
        map += ("fieldName" -> fieldName)
      }
      if (fieldType.isDefined) {
        map += ("fieldType" -> fieldType)
      }
      map.result()
    }
  }

  case class ConstantExpr(value: Double) extends AST {

    override def toJsonObj: Map[String, AnyRef] = Map[String, AnyRef]("constant" -> value.toString)

    override def eval(
      sketchGroup: SketchGroup,
      step: Duration
    ): Map[String, EvalResult] = {
      Map.empty
    }

    override def label(tags: Map[String, Any]): String = value.toString
  }

  private def toBasicFilter(jsonNode: JsonNode): Filter = {
    val key = Option(jsonNode.get("k")).map(_.textValue())
    if (key.isEmpty) throw new IllegalArgumentException(s"No `k` provided in filter!")
    val op = Option(jsonNode.get("op")).map(_.textValue())
    if (op.isEmpty) throw new IllegalArgumentException(s"No op provided for filter!")
    val values = Option(jsonNode.get("v")).map(_.elements().asScala.map(_.textValue()).toList).getOrElse(List())
    if (values.isEmpty && !op.contains(EXISTS))
      throw new IllegalArgumentException(s"No value for key = $key provided in filter!")
    val extracted = Option(jsonNode.get("extracted")).exists(_.booleanValue())
    val computed = Option(jsonNode.get("computed")).exists(_.booleanValue())
    val dataType = Option(jsonNode.get("dataType")).map(_.textValue()).getOrElse(STRING_TYPE)
    Filter(k = key.get, v = values, op = op.get, extracted = extracted, computed = computed, dataType = dataType)
  }

  def toBaseExpr(payload: String): BaseExpr = {
    val objectMapper = new ObjectMapper()
    val jsonNode = objectMapper.readTree(payload)
    toBaseExpr(Option(jsonNode.get("id")).map(_.textValue()).getOrElse("_"), jsonNode)
  }

  def toBaseExpr(id: String, jsonNode: JsonNode): BaseExpr = {
    val dataset = Option(jsonNode.get("dataset")).map(_.textValue()).getOrElse("metrics")
    val metricType = Option(jsonNode.get("metricType"))
      .map(_.textValue())
      .map { metricTypeStr =>
        MetricType.fromStr(metricTypeStr)
      }
      .getOrElse(GAUGE)

    val extractor = Option(jsonNode.get("extract")).flatMap(ext => {
      if (ext != null && !ext.isNull) {
        Some(
          Extractor(
            regex = ext.get("regex").textValue(),
            fields = ext
              .get("fields")
              .elements()
              .asScala
              .toList
              .map(fieldNode => {
                val name = fieldNode.get("name").textValue()
                val dataType = fieldNode.get("type").textValue()
                ExtractedField(name, dataType)
              })
          )
        )
      } else None
    })
    val compute = Option(jsonNode.get("compute")).flatMap(computeNode => {
      val objectMapper = new ObjectMapper()
      if (!computeNode.isNull) {
        Some(
          Compute(
            labelName = computeNode.get("labelName").textValue(),
            functionCall =
              ComputeFunction.toFunctionCall(objectMapper.writeValueAsString(computeNode.get("functionCall")))
          )
        )
      } else {
        None
      }
    })
    val chartOpts = Option(jsonNode.get("chart")).map(chartOptsNode => {
      val groupBys = Option(chartOptsNode.get("groupBys"))
        .filter(_.isArray)
        .map(_.elements())
        .map(_.asScala.toList)
        .getOrElse(List[JsonNode]())
        .map(_.textValue())
      val aggregation = Option(chartOptsNode.get("aggregation"))
        .filter(_.isTextual)
        .map(_.textValue())
        .getOrElse("sum")
      val rollupAgg = Option(chartOptsNode.get("rollup")).map(_.textValue())
      val chartType = Option(chartOptsNode.get("type")).map(_.textValue()).getOrElse("count")
      ChartOptions(
        groupBys = groupBys,
        aggregation = aggregation,
        rollupAggregation = rollupAgg,
        `type` = ChartType.fromStr(chartType),
        fieldName = Option(chartOptsNode.get("fieldName")).map(_.textValue()),
        fieldType = Option(chartOptsNode.get("fieldType")).map(_.textValue())
      )
    })
    val order = Option(jsonNode.get("order")).map(_.textValue()).orElse(Some(DESCENDING))
    val limit = Option(jsonNode.get("limit")).map(_.intValue()).orElse(Some(1000))
    val filter = jsonNode.get("filter")
    if (filter == null) throw new IllegalArgumentException("No filter provided!")
    val queryClause = handleFilter(filter)
    BaseExpr(
      id = id,
      filter = queryClause,
      dataset = dataset,
      extractor = extractor,
      compute = compute,
      chartOpts = chartOpts,
      order = order,
      limit = limit,
      metricType = metricType,
      returnResults = Option(jsonNode.get("returnResults")).forall(_.booleanValue())
    )
  }

  private def toBinaryClauseFromFilterJsonNode(filter: JsonNode): QueryClause = {
    val op = filter.get("op")
    if (op == null) throw new IllegalArgumentException("No `op` provided in binary query clause!")
    val clauses = List.newBuilder[QueryClause]
    val filterElements = filter.elements().asScala
    for (element <- filterElements) {
      if (!element.isTextual) {
        clauses += handleFilter(element)
      }
    }
    val clausesResult = clauses.result()
    val numClauses = clausesResult.size
    if (clausesResult.isEmpty || numClauses < 2) {
      throw new IllegalArgumentException("Atleast two clauses required in a binary clause!")
    }

    var binaryClause: QueryClause = null
    for (c <- clausesResult) {
      if (binaryClause == null) {
        binaryClause = c
      } else {
        binaryClause = BinaryClause(q1 = binaryClause, q2 = c, op = op.textValue())
      }
    }
    binaryClause
  }

  private def handleFilter(filter: JsonNode): QueryClause = {
    Option(filter.get("not")) match {
      case Some(embedded) =>
        NotClause(not = handleFilter(embedded))
      case None =>
        if (filter.get("k") != null) {
          toBasicFilter(filter)
        } else {
          toBinaryClauseFromFilterJsonNode(filter)
        }
    }
  }

  def toASTInput(payload: String): ASTInput = {
    Json.decode[ASTInput](payload)
  }

  def toQuery(q: QueryClause, skipExtractedComputed: Boolean, overrideName: Boolean = false): Query = {
    q match {
      case ASTUtils.Filter(k, v, op, extracted, computed, _) =>
        if (skipExtractedComputed && (extracted || computed)) {
          Query.TRUE
        } else {
          val modK = if (k == NAME && overrideName) "name" else k
          op match {
            case EQ           => EqualsQuery(modK, v.head)
            case NOT_EQUALS   => NotQuery(EqualsQuery(modK, v.head))
            case LE           => LessThanEqualsQuery(modK, v.head)
            case LT           => LessThanQuery(modK, v.head)
            case GT           => GreaterThanQuery(modK, v.head)
            case GE           => GreaterThanEqualsQuery(modK, v.head)
            case REGEX        => RegexQuery(modK, v.head)
            case IN           => InQuery(modK, v.toSet)
            case NOT_IN       => NotQuery(InQuery(modK, v.toSet))
            case CONTAINS     => ContainsQuery(modK, v.head)
            case HAS | EXISTS => HasQuery(modK)
          }
        }
      case ASTUtils.BinaryClause(q1, q2, op) =>
        op match {
          case "and" =>
            val query1 = toQuery(q1, skipExtractedComputed, overrideName)
            val query2 = toQuery(q2, skipExtractedComputed, overrideName)
            val andQuery = AndQuery(query1, query2)
            andQuery
          case "or" =>
            OrQuery(toQuery(q1, skipExtractedComputed, overrideName), toQuery(q2, skipExtractedComputed, overrideName))
        }
      case ASTUtils.NotClause(not) => NotQuery(toQuery(not, skipExtractedComputed, overrideName))
    }
  }

  private def replaceClause(clause: QueryClause, replace: QueryClause, `with`: QueryClause): QueryClause = {
    if (clause.equals(replace)) {
      `with`
    } else {
      clause match {
        case BinaryClause(q1, q2, op) =>
          BinaryClause(replaceClause(q1, replace, `with`), replaceClause(q2, replace, `with`), op)
        case NotClause(not) =>
          NotClause(replaceClause(not, replace, `with`))
        case f: Filter =>
          f
      }
    }
  }

  private def findClause(clause: QueryClause, key: String): Option[QueryClause] = {
    clause match {
      case BinaryClause(q1, q2, _) =>
        findClause(q1, key).orElse(findClause(q2, key))
      case NotClause(not) => findClause(not, key)
      case f: Filter =>
        if (f.k.equals(key)) Some(f) else None
    }
  }

}
