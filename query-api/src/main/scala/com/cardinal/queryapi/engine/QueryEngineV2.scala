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

package com.cardinal.queryapi.engine

import akka.NotUsed
import akka.actor.{ActorSystem, Scheduler}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.sse.EventStreamParser
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import com.cardinal.config.StorageProfileCache
import com.cardinal.dbutils.DBDataSources
import com.cardinal.eval.EvalUtils.astEvalFlow
import com.cardinal.logs.LogCommons._
import com.cardinal.model._
import com.cardinal.model.query.common.SequencingStrategy.computeReplaySequence
import com.cardinal.model.query.common._
import com.cardinal.queryapi.engine.QueryEngineV2.{mergeSortedSource, toSegmentRequests, TEN_SEC_DURATION}
import com.cardinal.queryapi.engine.SegmentCacheManager.updateMetadataLookupTime
import com.cardinal.queryapi.engine.SegmentSequencer.allSources
import com.cardinal.utils.Commons._
import com.cardinal.utils.ast.ASTUtils._
import com.cardinal.utils.ast.BaseExpr.{isTagSynthetic, CARDINALITY_ESTIMATE_AGGREGATION}
import com.cardinal.utils.ast.FormulaListener.toFormulaAST
import com.cardinal.utils.ast.SketchTags.MAP_SKETCH_TYPE
import com.cardinal.utils.ast._
import com.cardinal.utils.ast.queries.NLPUtils.computeSegmentIds
import com.cardinal.utils.ast.queries.TrigramQuery
import com.cardinal.utils.{Commons, SpringContextUtil}
import com.netflix.atlas.json.Json
import com.typesafe.config.Config
import org.apache.datasketches.hll.{HllSketch, TgtHllType, Union}
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.time.Duration
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

object QueryEngineV2 {
  val TEN_SEC_DURATION: Duration = Duration.parse("PT10S")

  private val logger = LoggerFactory.getLogger(getClass)
  private val storageProfileCache = SpringContextUtil.getBean(classOf[StorageProfileCache])

  def toRegionalCNames(customerId: String): List[String] = {
    storageProfileCache
      .getStorageProfilesByOrgId(customerId)
      .map(sp => (sp.region, sp.cloudProvider))
      .distinct
      .map { c =>
        s"query-api.${c._1}.${c._2}.$CARDINALHQ_ENVIRONMENT.cardinalhq.net"
      }
  }

  def mergeSortedSource(
    sources: List[Source[Either[DataPoint, SketchInput], Any]],
    reverseSort: Boolean
  ): Source[Either[DataPoint, SketchInput], Any] = {
    implicit val ord: Ordering[Either[DataPoint, SketchInput]] = (x, y) => {
      val xTimestamp = x match {
        case Left(value)  => value.timestamp
        case Right(value) => value.timestamp
      }
      val yTimestamp = y match {
        case Left(value)  => value.timestamp
        case Right(value) => value.timestamp
      }
      if (reverseSort) {
        xTimestamp.compare(yTimestamp) * -1
      } else {
        xTimestamp.compare(yTimestamp)
      }
    }

    sources.fold(Source.empty)((s1, s2) => s1.mergeSorted(s2))
  }

  def toSegmentRequests(
    baseExpr: BaseExpr,
    step: Long,
    isTagQuery: Boolean,
    segments: List[SegmentInfo],
    tagDataType: Option[TagDataType]
  ): List[SegmentRequest] = {
    segments.map(
      segment => {
        segment.toSegmentRequest(queryTags = baseExpr.queryTags())
      }
    )
  }

  def sourceFromRemote[R](
    uri: String,
    func: ByteString => R,
    ip: String,
    pushDownRequest: PushDownRequest,
    useHttps: Boolean = false,
    headers: Seq[RawHeader] = List.empty
  )(implicit as: ActorSystem): Source[R, NotUsed] = {
    Source
      .single(pushDownRequest)
      .map(
        payload =>
          HttpRequest(
            method = HttpMethods.POST,
            uri = uri,
            headers = headers,
            entity = HttpEntity(PushDownRequest.toJson(payload))
        )
      )
      .via(toRequestFlow(ip, useHttps))
      .flatMapConcat(resp => resp.entity.dataBytes)
      .via(EventStreamParser(Int.MaxValue, Int.MaxValue))
      .map(sse => Json.decode[Map[String, AnyRef]](sse.data))
      .filter(map => map.contains("message"))
      .map(map => map("message"))
      .map(anyRef => Json.encode[AnyRef](anyRef))
      .map(sse => ByteString(sse))
      .map(func.apply)
      .recoverWithRetries(1, {
        case e: Exception =>
          logger.error(s"Error sourcing from $ip:$uri", e)
          Source.empty[R]
      })
  }

  private def toRequestFlow(
    podIp: String,
    useHttps: Boolean
  )(implicit as: ActorSystem): Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]] = {
    if (useHttps) {
      Http().outgoingConnectionHttps(host = podIp)
    } else {
      Http().outgoingConnection(host = podIp, port = 7101)
    }
  }
}

class QueryEngineV2(
  actorSystem: ActorSystem,
  config: Config,
  segmentCacheManager: SegmentCacheManager,
  storageProfileCache: StorageProfileCache
) {
  implicit val as: ActorSystem = actorSystem
  implicit val ec: ExecutionContext = as.dispatcher
  implicit val scheduler: Scheduler = as.scheduler
  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  // This method sources the provided `segments` list, for the baseExpr by pushing down regional query/query-workers
  // The return type can either be `DataPoint` for exemplar queries or can be `SketchInput` for aggregates.
  private def sourceBaseExpr(
    queryId: String,
    baseExpr: BaseExpr,
    reverseSort: Boolean,
    stepInMillis: Long,
    segments: List[SegmentInfo],
    isTagQuery: Boolean = false
  ): Source[Either[DataPoint, SketchInput], NotUsed] = {
    Source
      .single(segments)
      .map(
        segments =>
          toSegmentRequests(
            baseExpr = baseExpr,
            step = stepInMillis,
            isTagQuery = isTagQuery,
            segments = segments,
            tagDataType = None
        )
      )
      .map(segments => {
        val sources = allSources(
          queryId = queryId,
          baseExpr = baseExpr,
          segmentRequests = segments,
          segmentCacheManager = segmentCacheManager,
          tagDataType = None,
        )
        mergeSortedSource(sources.toList, reverseSort)
      })
      .flatMapConcat(source => source)
      .recover {
        case e: Exception =>
          logger.error(s"Error in dataExpr eval ${e.getMessage}", e)
          throw new RuntimeException(e)
      }
  }

  def evaluate(
    astInput: ASTInput,
    startDateTime: Long,
    endDateTime: Long,
    step: Duration,
    customerId: String,
    queryId: String
  ): Source[GenericSSEPayload, NotUsed] = {
    val baseExprSources = astInput.baseExpressions.filter(_._2.returnResults).map {
      case (baseExprId, baseExpr) =>
        baseExpr.chartOpts match {
          case Some(_) =>
            segmentSource(
              baseExpressions = List(baseExpr.copy(id = baseExprId)),
              frequency = step,
              startDateTime = startDateTime,
              endDateTime = endDateTime,
              customerId = customerId,
              queryId = queryId
            ).flatMapConcat(Source(_))
              .flatMapMerge(
                3,
                segmentGroup => {
                  evaluateBaseExpr(
                    queryId = queryId,
                    customerId = customerId,
                    step = step,
                    baseExprId = baseExprId,
                    baseExpr = baseExpr,
                    segments = segmentGroup.segments
                  ).via(toGenericSSEPayload(id = baseExprId, ast = baseExpr))
                }
              )
          case None =>
            streamExemplars(
              queryId = queryId,
              startDateTime = startDateTime,
              endDateTime = endDateTime,
              step = step,
              baseExprId = baseExprId,
              baseExpr = baseExpr,
              customerId = customerId
            )
        }
    }
    val formulaSources = astInput.formulae.map(
      formula =>
        evaluateFormula(
          queryId = queryId,
          formula = formula,
          baseExpressions = astInput.baseExpressions,
          start = startDateTime,
          end = endDateTime,
          step = step,
          customerId = customerId
      )
    )
    (baseExprSources ++ formulaSources).fold(Source.empty)((s1, s2) => s1.merge(s2))
  }

  private def evaluateBaseExpr(
    queryId: String,
    customerId: String,
    step: Duration,
    baseExprId: String,
    baseExpr: BaseExpr,
    segments: List[SegmentInfo],
  ): Source[Map[String, EvalResult], Any] = {
    val copts = baseExpr.chartOpts.get
    val aggregations = copts.aggregation match {
      case AVG => Set(SUM, COUNT)
      case _   => Set(copts.aggregation)
    }

    val sources = aggregations.map(
      aggregation =>
        sourceBaseExpr(
          baseExpr = baseExpr.copy(chartOpts = Some(copts.copy(aggregation = aggregation))),
          reverseSort = false, // For aggregate queries, this should be false because of how time grouping works in evalASTFlow
          stepInMillis = step.toMillis,
          segments = segments,
          queryId = queryId
      )
    )
    // For aggregate queries, reverseSort should be false because of how time grouping works in evalASTFlow
    val merged = mergeSortedSource(sources.toList, reverseSort = false)
      .map {
        case Left(_)      => None
        case Right(value) => Some(value)
      }
      .filter(_.isDefined)
      .map(_.get)
      .map(List(_))

    merged.via(
      evalASTFlow(ast = baseExpr, func = _ => Some(baseExpr), step = step)
    )
  }

  private def evaluateFormula(
    formula: String,
    baseExpressions: Map[String, BaseExpr],
    start: Long,
    end: Long,
    step: Duration,
    customerId: String,
    queryId: String
  ): Source[GenericSSEPayload, NotUsed] = {
    val formulaAST = toFormulaAST(formula, baseExpressions)
    segmentSource(
      baseExpressions = getBaseExpressionsUsedInAST(formulaAST),
      frequency = step,
      startDateTime = start,
      endDateTime = end,
      customerId = customerId,
      queryId = queryId
    ).flatMapConcat(Source(_))
      .flatMapConcat { segmentGroup =>
        // - Take all the baseExpressions used in the formula, and call evaluateBaseExpr() for each.
        // - mergeSort the outputs, and feed astEvalFlow(ast) where ast = formulaAST.
        val groupedByBaseExpr = segmentGroup.segments.groupBy(_.exprId)
        val baseExprSources = groupedByBaseExpr.map {
          case (baseExprId, segments) =>
            val baseExpr = baseExpressions(baseExprId)
            evaluateBaseExpr(
              customerId = customerId,
              step = step,
              baseExprId = baseExprId,
              baseExpr = baseExpr,
              segments = segments,
              queryId = queryId
            ).flatMapConcat(
              resultSet => {
                val baseExprOutputTurnedIntoSketchInputs = resultSet.values
                  .map(
                    evalResult =>
                      SketchInput(
                        customerId = customerId,
                        baseExprHashCode = baseExprId,
                        timestamp = evalResult.timestamp,
                        sketchTags = SketchTags(
                          tags = evalResult.tags,
                          sketchType = MAP_SKETCH_TYPE,
                          sketch = Right(Map[String, Double](SUM -> evalResult.value))
                        )
                    )
                  )
                  .toList

                Source(baseExprOutputTurnedIntoSketchInputs)
                  .map(Right(_))
              }
            )
        }.toList

        // Since formula.eval is arithmetic calculation only, rewrite formula AST to only use aggregation = sum
        val modifiedFormulaAST = toFormulaAST(formula, baseExpressions.map {
          case (id, baseExpr) =>
            id -> baseExpr.copy(chartOpts = baseExpr.chartOpts.map(copts => copts.copy(aggregation = SUM)))
        })
        val modifiedBaseExpressions = getBaseExpressionsUsedInAST(modifiedFormulaAST).map(b => b.id -> b).toMap
        mergeSortedSource(baseExprSources, reverseSort = false)
          .map {
            case Left(_)      => None
            case Right(value) => Some(value)
          }
          .filter(_.isDefined)
          .map(_.get)
          .map(List(_))
          .via(
            astEvalFlow(
              ast = modifiedFormulaAST,
              baseExprLookup = (sketchInput: SketchInput) => modifiedBaseExpressions.get(sketchInput.baseExprHashCode),
              step = step
            )
          )
          .via(toGenericSSEPayload(id = formula, ast = formulaAST))
      }
  }

  private def evalASTFlow(
    ast: AST,
    func: SketchInput => Option[BaseExpr],
    step: Duration
  ): Flow[List[SketchInput], Map[String, EvalResult], NotUsed] = {
    Flow[List[SketchInput]]
      .via(astEvalFlow(ast, baseExprLookup = func, step = step))
  }

  private def toGenericSSEPayload(id: String, ast: AST): Flow[Map[String, EvalResult], GenericSSEPayload, NotUsed] = {
    Flow[Map[String, EvalResult]]
      .flatMapConcat(
        map =>
          Source(map.values.map { evalResult =>
            GenericSSEPayload(
              id = id,
              `type` = "timeseries",
              message = Map[String, AnyRef](
                "timestamp" -> Long.box(evalResult.timestamp),
                "tags"      -> evalResult.tags,
                "value"     -> Double.box(evalResult.value),
                "label"     -> ast.label(evalResult.tags)
              )
            )
          }.toList)
      )
  }

  def evaluateTagQuery(
    tagDataType: Option[TagDataType],
    baseExpr: BaseExpr,
    startDateTime: Long,
    endDateTime: Long,
    step: Duration,
    customerId: String,
    queryId: String,
  ): Source[GenericSSEPayload, NotUsed] = {
    val modifiedStep = if (baseExpr.dataset != METRICS) TEN_SEC_DURATION else step
    var modifiedBaseExpr = tagDataType match {
      case Some(td) =>
        if (!isTagSynthetic(baseExpr, td.tagName)) {
          baseExpr.copy(
            filter = BinaryClause(
              q1 = baseExpr.filter,
              q2 = Filter(k = td.tagName, op = EXISTS, dataType = td.dataType, extracted = false, computed = false),
              op = "and"
            )
          )
        } else baseExpr
      case None => baseExpr
    }
    modifiedBaseExpr = modifiedBaseExpr.copy(chartOpts = None)

    val source = segmentSource(
      baseExpressions = List(modifiedBaseExpr),
      frequency = modifiedStep,
      startDateTime = startDateTime,
      endDateTime = endDateTime,
      customerId = customerId,
      queryId = queryId
    ).flatMapConcat(Source(_))
      .flatMapConcat { segmentGroup =>
        val segmentRequests = toSegmentRequests(
          baseExpr = modifiedBaseExpr,
          step = modifiedStep.toMillis,
          isTagQuery = true,
          segments = segmentGroup.segments,
          tagDataType = tagDataType
        )
        val sources = allSources(
          queryId = queryId,
          baseExpr = modifiedBaseExpr,
          segmentRequests = segmentRequests,
          segmentCacheManager = segmentCacheManager,
          isTagQuery = true,
          tagDataType = tagDataType,
          postPushDownProcessor = Some(PostPushDownProcessor(tagNameCompressionEnabled = true))
        )

        Source(sources)
          .flatMapMerge(
            Int.MaxValue,
            source =>
              source.map {
                case Left(datapoint) =>
                  Some(GenericSSEPayload(message = datapoint.tags))
                case Right(_) => None
            }
          )
          .recover {
            case e: Exception =>
              logger.error("Error in streaming", e)
              throw new RuntimeException(e)
          }
          .filter(_.isDefined)
          .map(_.get)
      }

//    TagQueryUtils.aggregate(customerId, tagDataType, source, metricInfoCache)
      source
  }

  private def streamExemplars(
    startDateTime: Long,
    endDateTime: Long,
    step: Duration,
    baseExprId: String,
    baseExpr: BaseExpr,
    customerId: String,
    queryId: String
  ): Source[GenericSSEPayload, NotUsed] = {
    // treat as a query for exemplars
    val modifiedBaseExpr = baseExpr.copy(chartOpts = None)
    segmentSource(
      baseExpressions = List(baseExpr.copy(id = baseExprId)),
      frequency = TEN_SEC_DURATION, // exemplars query raw data, so hard code to default frequency = 10s
      startDateTime = startDateTime,
      endDateTime = endDateTime,
      customerId = customerId,
      queryId = queryId
    ).flatMapConcat(Source(_))
      .flatMapConcat { segmentGroup =>
        val segmentRequests = toSegmentRequests(
          baseExpr = modifiedBaseExpr,
          step = step.toMillis,
          isTagQuery = false,
          segments = segmentGroup.segments,
          tagDataType = None
        )

        val shouldReverseSort = checkShouldReverseSort(baseExpr)

        val sources = allSources(
          queryId = queryId,
          reverseSort = shouldReverseSort,
          segmentRequests = segmentRequests,
          segmentCacheManager = segmentCacheManager,
          baseExpr = modifiedBaseExpr,
          tagDataType = None,
        )

        mergeSortedSource(sources.toList, reverseSort = shouldReverseSort)
          .map {
            case Left(datapoint) =>
              Some(dataPointToGenericSSEPayload(id = baseExprId, datapoint = datapoint))
            case Right(_) => None
          }
          .filter(_.isDefined)
          .map(_.get)
      }
      .take(baseExpr.limit.map(_.toLong).getOrElse(1000L))
  }

  private def dataPointToGenericSSEPayload(id: String, datapoint: DataPoint): GenericSSEPayload = {
    GenericSSEPayload(
      id = id,
      `type` = "event",
      message = Json.decode[Map[String, AnyRef]](Json.encode(datapoint))
    )
  }

  private def segmentSource(
    baseExpressions: List[BaseExpr],
    frequency: Duration,
    startDateTime: Long,
    endDateTime: Long,
    customerId: String,
    queryId: String
  ): Source[List[SegmentGroup], NotUsed] = {
    val shouldReverseSort = checkShouldReverseSort(baseExpressions.head)

    relevantSegments(
      baseExpressions,
      startDateTime,
      endDateTime,
      customerId = customerId,
      queryId = queryId,
      frequency = frequency.toMillis
    ).map { segments =>
        segmentCacheManager.enqueueCacheRequest(segments) // asynchronously cache segments
        segments
      }
      .filter(_.nonEmpty)
      .map(segments => {
        val segmentStartTs = segments.map(_.startTs).min
        val segmentEndTs = segments.map(_.endTs).max
        logger.info(
          s"[$queryId] Incoming numSegments = ${segments.size}, sealed = ${segments.size}," +
          s" startTs = ${new DateTime(segmentStartTs)}," +
          s" endTs = ${new DateTime(segmentEndTs)}"
        )
        computeReplaySequence(
          segments,
          reverseSort = shouldReverseSort,
          startTs = startDateTime,
          endTs = endDateTime,
          frequency = frequency,
          executionGroupSize = executionGroupSize(segmentCacheManager)
        )
      })
  }

  private def executionGroupSize(segmentCacheManager: SegmentCacheManager): Int = {
    val numWorkers = Math.max(6, segmentCacheManager.readyPods)
    numWorkers * config.getInt("query.worker.num.file.capacity.per.vCPU") * config.getInt("query.worker.num.vCPU")
  }

  def computeCardinality(
    mainBaseExpr: BaseExpr,
    s: Long,
    e: Long,
    customerId: String,
    queryId: String = ""
  ): Source[Double, NotUsed] = {
    segmentSource(
      baseExpressions = List(mainBaseExpr),
      frequency = Duration.parse("PT60S"),
      startDateTime = s,
      endDateTime = e,
      customerId = customerId,
      queryId = queryId
    ).flatMapConcat(Source(_))
      .flatMapConcat(
        segmentGroup => {
          val modifiedBaseExpr = mainBaseExpr.copy(
            chartOpts = mainBaseExpr.chartOpts.map(_.copy(rollupAggregation = Some(CARDINALITY_ESTIMATE_AGGREGATION)))
          )
          val segmentRequests = toSegmentRequests(
            baseExpr = modifiedBaseExpr,
            step = 60000,
            isTagQuery = false,
            segments = segmentGroup.segments,
            tagDataType = None
          )

          val sources = allSources(
            queryId = queryId,
            segmentRequests = segmentRequests,
            segmentCacheManager = segmentCacheManager,
            baseExpr = modifiedBaseExpr,
            tagDataType = None
          )

          Source(sources)
            .flatMapMerge(
              Int.MaxValue,
              source =>
                source
                  .map {
                    case Left(_) => None
                    case Right(value) =>
                      value.sketchTags.sketch match {
                        case Left(value) => Some(value)
                        case Right(_)    => None
                      }
                  }
                  .filter(_.isDefined)
                  .map(_.get)
            )
        }
      )
      .statefulMapConcat { () =>
        var existingSketch = new HllSketch(12, TgtHllType.HLL_4)
        var currentEstimate: Double = 0.0
        bytes =>
          val incomingSketch = HllSketch.heapify(bytes)
          val union = new Union()
          union.update(incomingSketch)
          union.update(existingSketch)
          existingSketch = union.getResult
          val newEstimate = existingSketch.getEstimate
          if (currentEstimate == 0.0 || currentEstimate != newEstimate) {
            currentEstimate = newEstimate
            Some(newEstimate)
          } else None
      }
  }

  def computeCardinality(
    serviceName: Option[String],
    metricName: String,
    tags: Set[String],
    s: Long,
    e: Long,
    customerId: String
  ): Source[Double, NotUsed] = {
    val filter = serviceName match {
      case Some(s) =>
        BinaryClause(
          q1 = Filter(k = SERVICE, v = List(s), op = EQ, extracted = false, computed = false),
          q2 = Filter(k = NAME, v = List(metricName), op = EQ, extracted = false, computed = false),
          op = "and"
        )
      case None => Filter(k = NAME, v = List(metricName), op = EQ, extracted = false, computed = false)
    }

    val mainBaseExpr = BaseExpr(
      id = s"${serviceName}_$metricName",
      dataset = METRICS,
      filter = filter,
      extractor = None,
      compute = None,
      chartOpts = Some(ChartOptions(groupBys = tags.toList, aggregation = SUM))
    )

    computeCardinality(mainBaseExpr = mainBaseExpr, s = s, e = e, customerId = customerId)
  }

  private def relevantSegments(
    baseExpressions: List[BaseExpr],
    startDateTime: Long,
    endDateTime: Long,
    customerId: String,
    queryId: String,
    frequency: Long
  ): Source[List[SegmentInfo], NotUsed] = {
    val startTs = startDateTime
    val endTs = endDateTime
    val fpRequest = FingerprintRequest(
      startTs = startTs,
      endTs = endTs,
      baseExpressions = baseExpressions,
      customerId = customerId,
      queryId = queryId,
      frequency = frequency
    )
    readIndex(request = fpRequest)
  }

  private def readIndex(request: FingerprintRequest): Source[List[SegmentInfo], NotUsed] = {
    val hoursByDateInt = request.hoursByDateInt
    val trigramsQueriesByBaseExpr = request.trigramsQueriesByBaseExpr
    if (request.baseExpressions.isEmpty) {
      Source.single(List.empty)
    } else {
      val baseExpressions = request.baseExpressions

      val shouldReverseSort = checkShouldReverseSort(baseExpressions.head)
      val sortedDateInts = hoursByDateInt.keySet.toList.sortWith((date1, date2) => {
        if (shouldReverseSort) date1.toLong > date2.toLong
        else date1.toLong < date2.toLong
      })

      Source(sortedDateInts)
        .map { dateInt =>
          val segmentsResult = Set.newBuilder[SegmentInfo]
          baseExpressions.foreach { baseExpr =>
            val frequencyToUse = request.frequency
            if (baseExpr.dataset == METRICS) {
              val startTs = request.startTs
              val endTs = request.endTs
              val query = s"SELECT instance_num, segment_id, lower(ts_range) AS start_ts, upper(ts_range) - 1 AS end_ts FROM metric_seg" +
                s" WHERE ts_range && int8range($startTs, $endTs, '[)')" +
                s" AND dateint = $dateInt" +
                s" AND frequency_ms = $frequencyToUse" +
                s" AND organization_id = '${request.customerId}'" +
                s" AND published = true"
              val s = System.currentTimeMillis()
              val connection = DBDataSources.getLRDBSource.getConnection

              val statement = connection.prepareStatement(query)
              val resultSet = statement.executeQuery()
              logger.info(s"Metrics Metadata Query = $query, customerId = ${request.customerId}, dateInt = $dateInt, frequency = $frequencyToUse")
              while (resultSet.next()) {
                val instanceNum = resultSet.getInt("instance_num")
                val startTs = resultSet.getLong("start_ts")
                val endTs = resultSet.getLong("end_ts")
                val segmentId = s"tbl_${resultSet.getLong("segment_id")}"

                val (_, endHour) = Commons.toDateIntHour(endTs)

                storageProfileCache.getStorageProfile(request.customerId, instanceNum) match {
                  case Some(storageProfile) =>
                    segmentsResult += SegmentInfo(
                      dateInt = dateInt,
                      hour = endHour,
                      segmentId = s"$segmentId",
                      sealedStatus = true,
                      startTs = startTs,
                      endTs = endTs,
                      frequency = frequencyToUse,
                      exprId = baseExpr.id,
                      dataset = baseExpr.dataset,
                      customerId = request.customerId,
                      bucketName = storageProfile.bucket,
                      collectorId = storageProfile.collectorId.getOrElse(
                        throw new IllegalArgumentException(
                          s"No collectorId found while processing storage profile ${storageProfile.storageProfileId}"
                        )
                      )
                    )
                  case None =>
                    logger.warn(s"Storage profile not found for customerId: ${request.customerId}, instanceNum: $instanceNum")
                }
              }
              val e = System.currentTimeMillis()
              updateMetadataLookupTime(e - s)
              statement.close()
              connection.close()
            } else {
              val requiredHoursForThisDateInt = hoursByDateInt(dateInt).flatMap(_.hours).toSet
              val sortedHours = requiredHoursForThisDateInt.toList
                .sortWith((hour1, hour2) => {
                  if (shouldReverseSort) hour1.toLong > hour2.toLong
                  else hour1.toLong < hour2.toLong
                })

              logger.info(s"BaseExpr: ${Json.encode(baseExpr)}")
              val (tq, fingerprints) = trigramsQueriesByBaseExpr(baseExpr)
              val start = System.currentTimeMillis()
              val futures = sortedHours.grouped(3).map { _ =>
                Future {
                  fetchLogSegments(
                    customerId = request.customerId,
                    baseExpr = baseExpr,
                    dateInt = dateInt,
                    skipSegmentResolution = request.skipSegmentResolution,
                    tq = tq,
                    fingerprints = fingerprints,
                    startTs = request.startTs,
                    endTs = request.endTs,
                    frequencyToUse = frequencyToUse
                  )
                }
              }
              val end = System.currentTimeMillis()
              updateMetadataLookupTime(end - start)
              val results = Try(Await.result(Future.sequence(futures), 40.seconds)).getOrElse(List())
              results.foreach { result =>
                segmentsResult ++= result
              }
            }
          }
          segmentsResult.result().toList
        }
    }
  }

  private def fetchLogSegments(
    customerId: String,
    baseExpr: BaseExpr,
    skipSegmentResolution: Boolean = false,
    dateInt: String,
    tq: TrigramQuery,
    fingerprints: Set[Long],
    startTs: Long,
    endTs: Long,
    frequencyToUse: Long
  ): List[SegmentInfo] = {
    var connection: Connection = null
    var statement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      val start = System.currentTimeMillis()
      connection = DBDataSources.getLRDBSource.getConnection
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)
      val segmentIdColumn = "segment_id"
      var tableName = ""
      if(baseExpr.dataset == LOGS) {
         tableName = "log_seg"
      } else {
        tableName = "trace_seg"
      }

      val query =
        s"""
          |SELECT
          |  t.fp                    AS fingerprint,
          |  s.instance_num,
          |  s.segment_id,
          |  lower(s.ts_range)       AS start_ts,
          |  upper(s.ts_range) - 1   AS end_ts
          |FROM $tableName AS s
          |  CROSS JOIN LATERAL
          |    unnest(s.fingerprints) AS t(fp)
          |WHERE
          |  s.organization_id = ?        -- UUID
          |  AND s.dateint      = ?       -- dateint
          |  AND s.fingerprints && ?::BIGINT[]
          |  AND t.fp           = ANY(?::BIGINT[])
          |  AND ts_range && int8range(?, ?, '[)')
          |""".stripMargin
      logger.info("Issuing query to fetch segments: " + query + " for customerId: " + customerId + ", dateInt: " + dateInt + ", fingerprints: " + fingerprints.mkString(", ")
        + "startTs: " + startTs + ", endTs: " + endTs + ", frequencyToUse: " + frequencyToUse)
      statement = connection.prepareStatement(query)

//      logger.info(s"Query = $dateInt, customerId = $customerId, fingerprints = ${fingerprints.mkString(", ")}")
      statement.setObject(1, UUID.fromString(customerId))
      statement.setInt(2, dateInt.toInt)
      statement.setArray(3, connection.createArrayOf("BIGINT", fingerprints.map(Long.box).toArray))
      statement.setArray(4, connection.createArrayOf("BIGINT", fingerprints.map(Long.box).toArray))
      statement.setLong(5, startTs)
      statement.setLong(6, endTs)

      resultSet = statement.executeQuery()
      val segmentInfoMap = new mutable.HashMap[String, SegmentInfo]()
      val fpToSegmentIds = new mutable.HashMap[Long, mutable.HashSet[SegmentInfo]]()

      while (resultSet.next()) {
        val fp = resultSet.getLong("fingerprint")
        val instanceNum = resultSet.getInt("instance_num")
        val segmentId = resultSet.getLong(segmentIdColumn)
        val startTs = resultSet.getLong("start_ts")
        val endTs = resultSet.getLong("end_ts")
        val (targetDateInt, targetHour) = Commons.toDateIntHour(startTs)

        storageProfileCache.getStorageProfile(customerId, instanceNum).foreach { storageProfile =>
          val id = s"${segmentId}_${customerId}_${storageProfile.collectorId}_${storageProfile.bucket}"
          val segmentInfo = segmentInfoMap.getOrElseUpdate(
            id, {
              SegmentInfo(
                dateInt = targetDateInt,
                hour = targetHour,
                segmentId = s"tbl_$segmentId",
                sealedStatus = true,
                startTs = startTs,
                endTs = endTs,
                exprId = baseExpr.id,
                dataset = baseExpr.dataset,
                customerId = customerId,
                bucketName = storageProfile.bucket,
                collectorId = storageProfile.collectorId.getOrElse(
                  throw new IllegalArgumentException(
                    s"No collectorId found while processing storage profile ${storageProfile.storageProfileId}"
                  )
                ),
                frequency = frequencyToUse,
              )
            }
          )
          val set = fpToSegmentIds.getOrElseUpdate(fp, new mutable.HashSet[SegmentInfo]())
          set.add(segmentInfo)
        }
      }
      val list = if (skipSegmentResolution) {
        fpToSegmentIds.values.flatten.toList
      } else {
        computeSegmentIds(trigramQuery = tq, fpToSegmentIds.map(e => e._1 -> e._2.toSet).toMap).toList
      }
      val end = System.currentTimeMillis()
      logger.info(s"Returning ${list.size} segments in ${end - start}ms")
      list
    } catch {
      case e: Exception =>
        logger.error("Error in metadata lookup", e)
        List()
    } finally {
      if (resultSet != null) resultSet.close()
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }
  def loadExemplarMetricsMetadataJson(orgId: String): String = {
    val sql =
      """
        |SELECT DISTINCT
        |  attributes->>'metric.name' AS metric_name,
        |  attributes->>'metric.type' AS metric_type
        |FROM exemplar_metrics
        |WHERE organization_id = ?
        |  AND attributes ? 'metric.name'
        |  AND attributes ? 'metric.type'
        |ORDER BY 1 ASC
    """.stripMargin

    var connection: Connection      = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet         = null
    val rows = scala.collection.mutable.ListBuffer.empty[Map[String, String]]

    try {
      connection = DBDataSources.getLRDBSource.getConnection
      connection.setReadOnly(true)
      preparedStatement = connection.prepareStatement(sql)
      preparedStatement.setFetchSize(500)
      preparedStatement.setObject(1, UUID.fromString(orgId))

      resultSet = preparedStatement.executeQuery()
      while (resultSet.next()) {
        rows += Map(
          "metricName" -> resultSet.getString("metric_name"),
          "metricType" -> Option(resultSet.getString("metric_type")).getOrElse("gauge")
        )
      }
      Json.encode(rows.toList)
    } finally {
      if (resultSet != null) resultSet.close()
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }
  }

  private def checkShouldReverseSort(baseExpr: BaseExpr): Boolean = {
    baseExpr.order.getOrElse(DESCENDING) == DESCENDING
  }
}
