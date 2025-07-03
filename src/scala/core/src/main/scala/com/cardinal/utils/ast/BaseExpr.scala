package com.cardinal.utils.ast

import com.cardinal.logs.LogCommons._
import com.cardinal.model.query.common._
import com.cardinal.model.query.pipeline.{Compute, Extractor, FunctionCall, Label}
import com.cardinal.utils.Commons._
import com.cardinal.utils.QuantityParser.parseQuantity
import com.cardinal.utils.RegexpStage
import com.cardinal.utils.ast.ASTUtils._
import com.cardinal.utils.ast.BaseExpr.getFromSketch
import com.datadoghq.sketch.ddsketch.proto.{DDSketch => DDSketchProto}
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore
import com.datadoghq.sketch.ddsketch.{DDSketch, DDSketchProtoBinding}
import com.fasterxml.jackson.annotation.JsonProperty
import com.netflix.atlas.json.Json
import org.apache.datasketches.hll.HllSketch

import java.time.Duration
import java.util.Objects
import java.util.concurrent.TimeUnit
import scala.collection.mutable

object BaseExpr {
  val CARDINALITY_ESTIMATE_AGGREGATION = "ces"
  private val METRICS_PROJECTIONS_WITH_VALUE: String = List(s"\"$TIMESTAMP\"", s"\"$NAME\"").mkString(", ")
  private val LOGS_PROJECTIONS: String =
    List(s"\"$TIMESTAMP\"", s"\"$VALUE\"", s"\"$NAME\"", s"\"$MESSAGE\"").mkString(", ")
  private val SPANS_PROJECTIONS: String =
    List(s"\"$TIMESTAMP\"", s"\"$VALUE\"", s"\"$SPAN_NAME\"", s"\"$SPAN_KIND\"").mkString(", ")

  def getFromSketch(
    either: Either[Array[Byte], Map[String, Double]],
    metricType: MetricType,
    aggregation: String,
    step: Duration
  ): Double = {
    either match {
      case Left(sketchBytes) =>
        aggregation match {
          case CARDINALITY_ESTIMATE_AGGREGATION =>
            val sketch = hllSketchFromBytes(sketchBytes)
            sketch.getEstimate
          case c if c.startsWith("p") =>
            val sketch = ddSketchFromBytes(sketchBytes)
            sketch.getValueAtQuantile(aggregation.replace("p", "").toDouble / 100.0)

          case MIN =>
            val sketch = ddSketchFromBytes(sketchBytes)
            sketch.getMinValue

          case MAX =>
            val sketch = ddSketchFromBytes(sketchBytes)
            sketch.getMaxValue

          case SUM =>
            val sketch = ddSketchFromBytes(sketchBytes)
            sketch.getSum

          case COUNT =>
            val sketch = ddSketchFromBytes(sketchBytes)
            sketch.getCount

          case AVG =>
            val sketch = ddSketchFromBytes(sketchBytes)
            if (sketch.getCount == 0) 0.0 else sketch.getSum / sketch.getCount

          case _ => throw new IllegalArgumentException(s"Invalid aggregation $aggregation")
        }

      case Right(aggregatedValues) =>
        aggregation match {
          case AVG =>
            val sum = aggregatedValues.getOrElse(SUM, Double.NaN)
            val count = aggregatedValues.getOrElse(COUNT, Double.NaN)
            sum / count
          case _ => aggregatedValues.getOrElse(aggregation, Double.NaN)
        }
    }
  }

  def ddSketchFromBytes(sketchBytes: Array[Byte]): DDSketch = {
    DDSketchProtoBinding.fromProto(
      () => new UnboundedSizeDenseStore(),
      DDSketchProto.parseFrom(sketchBytes)
    )
  }

  def hllSketchFromBytes(sketchBytes: Array[Byte]): HllSketch = {
    HllSketch.heapify(sketchBytes)
  }

  def generateSql(
    baseExpr: BaseExpr,
    startTs: Long,
    endTs: Long,
    isTagQuery: Boolean = false,
    tagDataType: Option[TagDataType] = None,
    stepInMillis: Long = Duration.parse("PT10S").toMillis,
    globalAgg: Option[String],
    nonExistentFields: Set[String]): String = {
    val baseSql = getBaseQuery(
      baseExpr = baseExpr,
      startTs = startTs,
      endTs = endTs,
      isTagQuery = isTagQuery,
      tagDataType = tagDataType,
      stepInMillis = stepInMillis,
      globalAgg = globalAgg,
      nonExistentFields = nonExistentFields)

    if (isTagQuery) {
      tagDataType match {
        case Some(t) =>
          val colName = s"\"${t.tagName}\""
          if(isTagSynthetic(baseExpr, t.tagName)) {
            s"SELECT $colName as \"${t.tagName}\", COUNT(*) AS count FROM ($baseSql) GROUP BY $colName"
          }
          else {
            val (filterSql, _, _) = toFilterExpr(baseExpr, nonExistentFields)
            val timestampSql = timestampFilter(startTs, endTs)
            s"SELECT $colName as \"${t.tagName}\", COUNT(*) AS count FROM {tableName}" +
              s" WHERE $filterSql AND $timestampSql GROUP BY $colName"
          }

        case None =>  baseSql
      }
    } else baseSql
  }

  def isTagSynthetic(baseExpr: BaseExpr, tagName: String): Boolean = {
    val syntheticFieldsAccumulator = new mutable.LinkedHashSet[Filter]()
    val existingFieldsAccumulator = new mutable.LinkedHashSet[Filter]()
    BaseExpr.filterSqlAndAccumulateFields(
      baseExpr.filter,
      syntheticFieldsAccumulator,
      syntheticFieldsAccumulator,
      existingFieldsAccumulator,
      Set.empty[String]
    )
    syntheticFieldsAccumulator.map(_.labelName).contains(tagName)
  }

  private def timestampFilter(startTs: Long, endTs: Long): String = {
    s"\"$TIMESTAMP\" >= $startTs AND \"$TIMESTAMP\" < $endTs"
  }

  private def toStepTsSql(stepInMillis: Long): String = {
    s"(\"$TIMESTAMP\" - (\"$TIMESTAMP\" % $stepInMillis.0)) as $STEP_TS"
  }

  private def toFilterExpr(baseExpr: BaseExpr, nonExistentFields: Set[String]): (String, mutable.LinkedHashSet[Filter], mutable.LinkedHashSet[Filter]) = {
    val extractedFieldsAccumulator = new mutable.LinkedHashSet[Filter]()
    val computedFieldsAccumulator = new mutable.LinkedHashSet[Filter]()
    val existingFieldsAccumulator = new mutable.LinkedHashSet[Filter]()
    val sql = filterSqlAndAccumulateFields(
      queryClause = baseExpr.filter,
      extractedFieldsAccumulator = extractedFieldsAccumulator,
      computedFieldsAccumulator = computedFieldsAccumulator,
      topLevelFieldsAccumulator = existingFieldsAccumulator,
      nonExistentFields = nonExistentFields,
    )
    (sql, extractedFieldsAccumulator, computedFieldsAccumulator)
  }

  private def getBaseQuery(
    baseExpr: BaseExpr,
    startTs: Long,
    endTs: Long,
    isTagQuery: Boolean,
    tagDataType: Option[TagDataType],
    stepInMillis: Long = Duration.parse("PT10S").toMillis,
    globalAgg: Option[String] = None,
    nonExistentFields: Set[String]): String = {
    // The QueryClause in the baseExpr has an 'exists' filter for all the extracted and computed fields. In a way, the
    // QueryClause (derived from the filter section of the request json) also acts as a projection list. Walk the
    // QueryClause and construct the final filter sql clause and as we are walking the QueryClause, we will also
    // accumulate the extracted, computed and existing fields that are either projected or used for computing and charting

    val (filterSqlString, extractedFieldsAccumulator, computedFieldsAccumulator) = toFilterExpr(baseExpr, nonExistentFields)

    // The final SQL has the following sub query structure
    // Chart-query (
    //   Compute-query (
    //     Extract-query (
    //       Projection-query with timestamp filter
    //     )
    //   )
    // )
    val timestampSql: String = timestampFilter(
      startTs = startTs,
      endTs = endTs
    )
    val projectionSql = baseExpr.dataset match {
      case LOGS    => LOGS_PROJECTIONS
      case METRICS => METRICS_PROJECTIONS_WITH_VALUE
      case SPANS   => SPANS_PROJECTIONS
      case _       => throw new IllegalArgumentException(s"Invalid dataset: ${baseExpr.dataset}")
    }
    var sqlStr = s"SELECT * FROM {tableName} WHERE $timestampSql".trim
    if (baseExpr.extractor.isDefined) {
      sqlStr = getExtractSql(baseExpr.extractor.get, extractedFieldsAccumulator, sqlStr)
    }
    if (baseExpr.compute.isDefined) {
      sqlStr = getComputeSql(baseExpr.compute.get, extractedFieldsAccumulator, sqlStr)
    }
    if (baseExpr.chartOpts.isDefined) {
      sqlStr = getChartSql(
        chart = baseExpr.chartOpts.get,
        stepMillis = stepInMillis,
        extractedFields = extractedFieldsAccumulator,
        computedFields = computedFieldsAccumulator,
        filterSql = filterSqlString,
        subQuery = sqlStr,
        globalAgg = globalAgg,
        dataset = baseExpr.dataset,
        nonExistentFields = nonExistentFields)
    } else {
      if (isTagQuery && tagDataType.isEmpty) {
        sqlStr = s"SELECT * FROM ($sqlStr) WHERE $filterSqlString"
      } else {
        sqlStr = s"SELECT $projectionSql, * FROM ($sqlStr) WHERE $filterSqlString ORDER BY \"$TIMESTAMP\" ${baseExpr.order
          .getOrElse(DESCENDING)}"
      }
    }
    sqlStr
  }

  private def getExtractSql(
    extractor: Extractor,
    extractedFields: mutable.LinkedHashSet[Filter],
    subQuery: String
  ): String = {
    val regexExtract =
      s" regexp_extract(replace(\"${extractor.inputField}\", '''', ''), '${extractor.regex.replace("'", "")}', [${extractor.fields
        .map(f => s"'${f.name}'")
        .mkString(", ")}]) as nlp_struct"

    val regexMatches =
      s" regexp_matches(replace(\"${extractor.inputField}\", '''', ''), '${extractor.regex.replace("'", "")}')"

    val extractedLabelToProjectionAndFilterSql: Map[String, (String, String)] =
      getExtractedLabelToProjectionAndFilterSqlMap(extractedFields)
    val extractProj = extractedLabelToProjectionAndFilterSql
      .map(labelToProjAndFilter => {
        labelToProjAndFilter._2._1 // pick only the projection sql and ignore filter sql
      })
      .mkString(", ")
    s"SELECT $extractProj, * FROM (SELECT $regexExtract, * FROM ($subQuery) WHERE $regexMatches)"
  }

  private def getComputeSql(
    compute: Compute,
    extractedFields: mutable.LinkedHashSet[Filter],
    subQuery: String
  ): String = {
    val computeLabelsAccumulator = new mutable.LinkedHashSet[Label]
    captureComputeLabels(compute.functionCall, computeLabelsAccumulator)
    val extractedLabelToProjectionAndFilterSql: Map[String, (String, String)] =
      getExtractedLabelToProjectionAndFilterSqlMap(extractedFields)
    val computeFilterSql = computeLabelsAccumulator
      .map(l => {
        extractedLabelToProjectionAndFilterSql
          .get(l.name)
          .map(projectAndFilterSql => {
            projectAndFilterSql._2
          })
          .getOrElse(s"${l.name}$$${l.dataType} IS NOT NULL")
      })
      .mkString(" AND ")
    val sql =
      s"SELECT (${compute.functionCall.toSql}) as ${compute.labelName}, * FROM ($subQuery) WHERE $computeFilterSql"
    sql
  }

  private def getExtractedLabelToProjectionAndFilterSqlMap(
    extractedFields: mutable.LinkedHashSet[Filter]
  ): Map[String, (String, String)] = {
    extractedFields
      .map(extractedF => {
        val labelName = extractedF.k
        labelName -> {
          val projectSql = s"nlp_struct['${extractedF.k}'] as $labelName"
          val filterSql = s"$labelName IS NOT NULL"
          (projectSql, filterSql)
        }
      })
      .toMap
  }

  private def captureComputeLabels(
    f: FunctionCall,
    computeArgs: mutable.LinkedHashSet[Label],
  ): Unit = {
    f.arguments.foreach {
      case l: Label =>
        computeArgs += l
      case f: FunctionCall =>
        captureComputeLabels(f, computeArgs)
      case _ =>
    }
  }

  private def getChartSql(
    chart: ChartOptions,
    stepMillis: Long,
    extractedFields: mutable.LinkedHashSet[Filter],
    computedFields: mutable.LinkedHashSet[Filter],
    filterSql: String,
    subQuery: String,
    globalAgg: Option[String],
    dataset: String,
    nonExistentFields: Set[String]
  ): String = {
    val extractorFieldNames: Set[String] = getExtractedLabelToProjectionAndFilterSqlMap(extractedFields).keySet
    val computeFieldNames: Set[String] = computedFields.map(_.k).toSet

    def isSyntheticField(g: String): Boolean = {
      extractorFieldNames.contains(g) || computeFieldNames.contains(g)
    }

    val stepTsSql = toStepTsSql(stepMillis)
    val existingFields = chart.groupBys.filter(g => !nonExistentFields.contains(g))
    val groupByProjectionSql = {
      if (chart.groupBys.nonEmpty && existingFields.nonEmpty) {
        s", ${existingFields.map(g => s"\"$g\"").mkString(", ")}"
      } else {
        ""
      }
    }
    val groupByGroupsSql = if (chart.groupBys.nonEmpty && existingFields.nonEmpty) s", ${existingFields.map(g => s"\"$g\"").mkString(", ")}" else ""

    val aggregationFunc = globalAgg.getOrElse(chart.aggregation)
    val aggValue = chart.fieldName.getOrElse(VALUE)
    val calc = aggValue match {
      case VALUE => s"$aggregationFunc(\"$VALUE\")"
      case _ =>
        if (chart.fieldName.isEmpty)
          throw new IllegalArgumentException("Required property: fieldName when chartType = `field`")
        if (chart.fieldType.isEmpty)
          throw new IllegalArgumentException("Required property: fieldType when chartType = `field`")
        val chartFieldName = chart.fieldName.get
        val chartFieldType = chart.fieldType.get
        var base =
          s"$aggregationFunc(try_cast(${if (extractorFieldNames.contains(chartFieldName) || computeFieldNames.contains(chartFieldName)) {
            chartFieldName
          } else s"$chartFieldName$$$chartFieldType"} as double))"
        if (chart.fieldType.contains(DURATION_TYPE)) {
          base = s"$base / ${TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS)}" // convert to ms
        } else if (chart.fieldType.contains(DATA_SIZE_TYPE)) {
          base = s"$base / 1000" // convert to KB
        }
        base
    }

    val chartFieldProjection = (chart.fieldName, chart.fieldType) match {
      case (Some(fieldName), Some(_)) => if(isSyntheticField(fieldName)) s", $fieldName" else s", $fieldName"
      case _                                  => ""
    }

    if (dataset == METRICS) {
      val rollupAggregation = chart.rollupAggregation.getOrElse(SUM)
      aggregationFunc match {
        case p if p.startsWith("p")  =>
          s"SELECT \"$TIMESTAMP\", $MAX(rollup_$rollupAggregation) as value," +
            s" \"$NAME\" as name $chartFieldProjection $groupByProjectionSql FROM ($subQuery) " +
            s" WHERE ${getChartFieldFilterSql(chart, extractorFieldNames, computeFieldNames)}" +
            s" AND $filterSql GROUP BY \"$TIMESTAMP\" $groupByGroupsSql, name ORDER BY \"$TIMESTAMP\" ASC"

        case CARDINALITY_ESTIMATE_AGGREGATION =>
          s"SELECT \"$TIMESTAMP\", 1.0 as value, \"$NAME\" as name $chartFieldProjection $groupByProjectionSql FROM ($subQuery) " +
            s" WHERE ${getChartFieldFilterSql(chart, extractorFieldNames, computeFieldNames)}" +
            s" AND $filterSql ORDER BY \"$TIMESTAMP\" ASC"

        case _ =>
          s"SELECT \"$TIMESTAMP\", $aggregationFunc(rollup_$rollupAggregation) as value," +
          s" \"$NAME\" as name $chartFieldProjection $groupByProjectionSql FROM ($subQuery) " +
          s" WHERE ${getChartFieldFilterSql(chart, extractorFieldNames, computeFieldNames)}" +
          s" AND $filterSql GROUP BY \"$TIMESTAMP\" $groupByGroupsSql, name  ORDER BY \"$TIMESTAMP\" ASC"
      }
    }
    else if (aggregationFunc.startsWith("p") || aggregationFunc.contains(CARDINALITY_ESTIMATE_AGGREGATION)) {
      s"SELECT \"$TIMESTAMP\", \"$VALUE\", \"$NAME\" as name $chartFieldProjection $groupByProjectionSql FROM ($subQuery) " +
        s" WHERE ${getChartFieldFilterSql(chart, extractorFieldNames, computeFieldNames)} AND $filterSql ORDER BY $TIMESTAMP ASC"
    } else {
      s"SELECT $stepTsSql, $calc, \"$NAME\" as name $groupByProjectionSql FROM ($subQuery) " +
      s" WHERE ${getChartFieldFilterSql(chart, extractorFieldNames, computeFieldNames)} AND $filterSql" +
      s" GROUP BY $STEP_TS $groupByGroupsSql, name ORDER BY $STEP_TS ASC"
    }
  }

  private def getChartFieldFilterSql(
    chart: ChartOptions,
    extractedFields: Set[String],
    computedFieldName: Set[String]
  ): String = {
    val filter = if (chart.fieldName.isDefined) {
      if (extractedFields.contains(chart.fieldName.get) || computedFieldName.contains(chart.fieldName.get)) {
        s"${chart.fieldName.get} IS NOT NULL"
      } else {
        s"${chart.fieldName.get}$$${chart.fieldType.get} IS NOT NULL"
      }
    } else {
      ""
    }
    if (filter.nonEmpty) {
      filter
    } else {
      "true"
    }
  }

  private val binaryClauseOp = Set("and", "or")
  private val normalizedDataTypes = Set(DURATION_TYPE, DATA_SIZE_TYPE, NUMBER_TYPE)

  // generate the filter sql by walking the QueryClause and also accumulate the extracted, computed and existing fields
  // based on the computed and extracted boolean in the Filter
  private def filterSqlAndAccumulateFields(
    queryClause: QueryClause,
    extractedFieldsAccumulator: mutable.LinkedHashSet[Filter],
    computedFieldsAccumulator: mutable.LinkedHashSet[Filter],
    topLevelFieldsAccumulator: mutable.LinkedHashSet[Filter],
    nonExistentFields: Set[String]
  ): String = {
    queryClause match {
      case f: Filter =>
        if (f.extracted) {
          extractedFieldsAccumulator += f
        } else if (f.computed) {
          computedFieldsAccumulator += f
        } else {
          topLevelFieldsAccumulator += f
        }

        if (normalizedDataTypes.contains(f.dataType) && f.v.size != 1) {
          throw new IllegalArgumentException(s"filter value is a list of values for dataType: ${f.dataType}")
        }

        def normalizedValue() = f.dataType match {
          case DURATION_TYPE  => parseQuantity(f.v.head, DURATION_TYPE).getOrElse(0.0)
          case DATA_SIZE_TYPE => parseQuantity(f.v.head, DATA_SIZE_TYPE).getOrElse(0.0)
          case NUMBER_TYPE    => f.v.head.toDouble
          case _              => Double.NaN
        }

        var labelName = f.labelName
        if(nonExistentFields.contains(labelName) && !f.extracted && !f.computed) {
          return "false"
        }
        if(labelName.contains(".")) {
          labelName = s"\"$labelName\""
        }

        f.op match {
          case HAS | EXISTS =>
            s"$labelName IS NOT NULL"

          case EQ =>
            s"$labelName = '${f.v.head}'"

          case NOT_EQUALS =>
            s"$labelName != '${f.v.head}'"

          case IN =>
            s"$labelName IN (${f.v.map(v => s"'$v'").mkString(", ")})"

          case NOT_IN =>
            s"$labelName NOT IN (${f.v.map(v => s"'$v'").mkString(", ")})"

          case REGEX =>
            s"regexp_matches($labelName, '${f.v.head}','i')"

          case GT =>
            s"$labelName > ${normalizedValue()}"

          case GE =>
            s"$labelName >= ${normalizedValue()}"

          case LT =>
            s"$labelName < ${normalizedValue()}"

          case LE =>
            s"$labelName <= ${normalizedValue()}"

          case CONTAINS =>
            s"regexp_matches($labelName, '.*${f.v.head}.*','i')"

          case _ => throw new IllegalArgumentException(s"Invalid operator ${f.op}")
        }
      case BinaryClause(q1, q2, op) =>
        if (!binaryClauseOp.contains(op)) {
          throw new IllegalArgumentException(s"unknown binary op $op")
        }
        s"(${filterSqlAndAccumulateFields(q1, extractedFieldsAccumulator, computedFieldsAccumulator, topLevelFieldsAccumulator, nonExistentFields)} $op ${filterSqlAndAccumulateFields(q2, extractedFieldsAccumulator, computedFieldsAccumulator, topLevelFieldsAccumulator, nonExistentFields)})"
      case NotClause(not) =>
        s"NOT (${filterSqlAndAccumulateFields(not, extractedFieldsAccumulator, computedFieldsAccumulator, topLevelFieldsAccumulator, nonExistentFields)})"
    }
  }
}

case class BaseExpr(
  @JsonProperty("id") id: String,
  @JsonProperty("dataset") dataset: String,
  @JsonProperty("filter") filter: QueryClause,
  @JsonProperty("extract") extractor: Option[Extractor] = None,
  @JsonProperty("compute") compute: Option[Compute] = None,
  @JsonProperty("chart") chartOpts: Option[ChartOptions] = None,
  @JsonProperty("limit") limit: Option[Int] = Some(1000),
  @JsonProperty("order") order: Option[String] = Some(DESCENDING),
  @JsonProperty("metricType") metricType: MetricType = GAUGE,
  @JsonProperty("returnResults") returnResults: Boolean = true
) extends AST {

  private def toFilterJsonObj(f: QueryClause): Map[String, AnyRef] = {
    f match {
      case f: Filter => Json.decode[Map[String, AnyRef]](Json.encode(f))
      case BinaryClause(q1, q2, op) =>
        val map = mutable.Map.empty[String, AnyRef]
        map += "q1" -> toFilterJsonObj(q1)
        map += "q2" -> toFilterJsonObj(q2)
        map += "op" -> op
        map.toMap

      case NotClause(not) =>
        val map = mutable.Map.empty[String, AnyRef]
        map += "not" -> toFilterJsonObj(not)
        map.toMap
    }
  }

  override def toJsonObj: Map[String, AnyRef] = {
    val map = Map.newBuilder[String, AnyRef]
    map += ("id"      -> id)
    map += ("dataset" -> dataset)
    map += ("filter"  -> toFilterJsonObj(filter))
    if (extractor.isDefined) {
      map += ("extract" -> extractor)
    }
    if (compute.isDefined) {
      map += ("compute" -> compute)
    }
    if (chartOpts.isDefined) {
      map += ("chart" -> chartOpts.map(_.toJsonObj))
    }
    map += ("limit"         -> limit)
    map += ("order"         -> order)
    map += ("metricType"    -> metricType.jsonString)
    map += ("returnResults" -> returnResults.asInstanceOf[AnyRef])
    map.result()
  }

  def isEventTelemetryType: Boolean = {
    dataset == LOGS || dataset == SPANS
  }

  private val extractorStage = extractor.map { logExtractor =>
    (
      logExtractor.inputField,
      logExtractor.fields.map(ef => ef.name -> ef.`type`).toMap,
      RegexpStage(regex = logExtractor.regex, names = logExtractor.fields.map(_.name))
    )
  }

  def transform(tags: mutable.Map[String, Any]): Double  = {
    extractorStage.foreach { e =>
      val (inputField, fieldTypeMap, regexpStage) = e
      val input = tags.getOrElseUpdate(inputField, "").toString
      if (input.nonEmpty) {
        regexpStage.eval(input).map { pair =>
          val dataType = fieldTypeMap(pair._1)
          val normalizedValue = dataType match {
            case NUMBER_TYPE => pair._2.toDouble
            case _           => pair._2
          }
          tags += pair._1 -> normalizedValue
        }
      }
    }

    compute.foreach { c =>
      {
        val computedFieldName = c.labelName
        val computedValue = c.functionCall.eval(tags.toMap)
        tags += computedFieldName -> computedValue
      }
    }

    var newValue: Double = 1.0
    chartOpts.foreach { chartOpt =>
      (chartOpt.fieldName, chartOpt.fieldType) match {
        case (Some(fieldName), Some(fieldType)) =>
          tags.get(fieldName) match {
            case Some(fieldValue) =>
              val normalizedFieldValue = fieldType match {
                case NUMBER_TYPE    => fieldValue.toString.toDouble
                case DURATION_TYPE  => parseQuantity(fieldValue, DURATION_TYPE).getOrElse(0.0)
                case DATA_SIZE_TYPE => parseQuantity(fieldValue, DATA_SIZE_TYPE).getOrElse(0.0)
              }
              newValue = normalizedFieldValue
            case None =>
          }
        case _ =>
      }
    }
    newValue
  }

  def queryTags(): Map[String, Any] = {
    exactTags(filter)
  }

  private def exactTags(q: QueryClause): Map[String, Any] = {
    val resultMap = Map.newBuilder[String, Any]
    q match {
      case Filter(k, v, op, _, _, _) =>
        op match {
          case EQ => resultMap += (k -> v.head)
          case IN  => resultMap += (k -> v)
          case _ =>
        }
      case BinaryClause(q1, q2, op) =>
        op match {
          case "and" =>
            resultMap ++= exactTags(q1)
            resultMap ++= exactTags(q2)
          case _ =>
        }
      case _ =>
    }
    resultMap.result()
  }

  def fieldSet(): Set[String] = {
    filterFieldSet(filter) ++ chartOpts.map(_.groupBys).getOrElse(Set())
  }

  private def filterFieldSet(q: QueryClause): Set[String] = {
    val set = Set.newBuilder[String]
    q match {
      case Filter(k, _, _, _, _, _) =>
        set += k
      case BinaryClause(q1, q2, _) =>
        set ++= filterFieldSet(q1)
        set ++= filterFieldSet(q2)
      case _ =>
    }
    set.result()
  }

  override def eval(
    sketchGroup: SketchGroup,
    step: Duration
  ): Map[String, EvalResult] = {
    val evalOutput = Map.newBuilder[String, EvalResult]
    val groupByKeys = getFinalGrouping(this)
    val sketchInputs = sketchGroup.group.getOrElse(this, List())
    val transformerFunc = this.chartOpts.map(_.`type`).map {
      chartType => getTransformerFunc(chartType, metricType, dataset, step.toMillis)
    }.getOrElse {
      (d: Double) => d
    }
    for (sketchInput <- sketchInputs) {
      val tags = sketchInput.sketchTags.tags
      val groupByKey = toGroupByKey(groupByKeys, tags)
      chartOpts.map(_.aggregation) match {
        case Some(aggregation) =>
          val value = transformerFunc.apply(getFromSketch(
            either = sketchInput.sketchTags.sketch,
            metricType = metricType,
            step = step,
            aggregation = aggregation,
          ))
          val evalResult = EvalResult(sketchInput.timestamp, value, tags = tags)
          if (groupByKeys.isEmpty) evalOutput += "default" -> evalResult
          else evalOutput += groupByKey                    -> evalResult
        case None =>
      }
    }
    evalOutput.result()
  }

  override def label(tags: Map[String, Any]): String = {
    val groupByKeys = getFinalGrouping(this)
    val sb = new mutable.StringBuilder()
    sb ++= "("
    if (groupByKeys.nonEmpty) {
      sb ++= groupByKeys.toList.sorted
        .map(key => {
          tags.get(key) match {
            case Some(value) => s"$key = $value"
            case None        => ""
          }
        })
        .filter(_.nonEmpty)
        .mkString(", ")
    } else {
      sb ++= filter.toString
    }
    sb ++= ")"
    sb.toString()
  }

  override def hashCode(): Int = {
    Objects.hash(dataset, filter, extractor, compute, chartOpts, metricType).hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: BaseExpr =>
        dataset.equals(other.dataset) && filter.equals(other.filter) && extractor.equals(other.extractor) &&
        compute.equals(other.compute) && chartOpts.equals(other.chartOpts) && metricType.equals(other.metricType)
      case _ => false
    }
  }
}
