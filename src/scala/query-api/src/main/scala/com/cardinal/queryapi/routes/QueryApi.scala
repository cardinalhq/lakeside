package com.cardinal.queryapi.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpEntity.ChunkStreamPart
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.{Route, StandardRoute}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.cardinal.auth.AuthToken
import com.cardinal.directives.AuthDirectives
import com.cardinal.instrumentation.Metrics.recordExecutionTime
import com.cardinal.logs.LogCommons.{EXISTS, STRING_TYPE}
import com.cardinal.model._
import com.cardinal.model.query.common.TagDataType
import com.cardinal.model.query.pipeline.ComputeFunction
import com.cardinal.queryapi.engine.QueryEngineV2.toRegionalCNames
import com.cardinal.queryapi.engine.SegmentCacheManager.updateTotalQueryTime
import com.cardinal.queryapi.engine.{QueryEngineV2, SegmentCacheManager}
import com.cardinal.utils.Commons
import com.cardinal.utils.Commons._
import com.cardinal.utils.ast.ASTUtils.{toASTInput, toBaseExpr, BinaryClause, Filter}
import com.cardinal.utils.ast.BaseExpr
import com.netflix.atlas.json.Json
import io.opentracing.util.GlobalTracer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.DependsOn
import org.springframework.stereotype.Component

import java.time.Duration
import java.util.UUID
import scala.concurrent.duration.DurationInt

@Component
@DependsOn(Array("storageProfileCache"))
class QueryApi @Autowired()(actorSystem: ActorSystem, queryEngine: QueryEngineV2) extends AuthDirectives(actorSystem) {
  private val logger = LoggerFactory.getLogger(getClass)

  private def scopeTags: Route = {
    path("api" / "v1" / "scopeTags") {
      get {
        complete(HttpEntity(ContentTypes.`application/json`, ByteString(Json.encode(Commons.INFRA_DIMENSIONS))))
      }
    }
  }

  private def healthy: Route = {
    path("ready") {
      complete(StatusCodes.OK)
    }
  }

  private def cardinalityApi: Route = {
    path("api" / "v1" / "cardinality") {
      post {
        auth { customerId =>
          entity(as[String]) { qBody: String =>
            {
              parameters("s".?, "e".?) {
                case (s, e) =>
                  val expr = toBaseExpr(qBody)
                  val (start: Long, end: Long) = toStartEnd(s, e)
                  val src = queryEngine
                    .computeCardinality(mainBaseExpr = expr, s = start, e = end, customerId = customerId)
                    .map(d => GenericSSEStringPayload(message = d.toString))

                  complete {
                    HttpEntity.Chunked(
                      MediaTypes.`text/event-stream`.toContentType,
                      Source.single(Done()).map(_.toChunkStreamPart).prepend(src.map(_.toChunkStreamPart))
                    )
                  }
              }
            }
          }
        }
      }
    }
  }

  private def tagsApi: Route = {
    path("api" / "v1" / "tags" / Segment) { dataset =>
      post {
        auth { customerId =>
          entity(as[String]) { qBody: String =>
            {
              parameters("tagName".?, "dataType".?, "s".?, "e".?, "limit".as[Long].?) {
                case (tagName, dataType, s, e, limit) =>
                  val startTs = System.currentTimeMillis()
                  if (Commons.isGlobalQueryStack) {
                    makeRegionalCall(
                      customerId = customerId,
                      uri = s"/api/v1/tags/$dataset",
                      params = Map(
                        "tagName"  -> tagName,
                        "dataType" -> dataType,
                        "s"        -> s,
                        "e"        -> e,
                        "limit"    -> limit.map(_.toString),
                      ),
                      qBody = qBody
                    )
                  } else {
                    val (start: Long, end: Long) = toStartEnd(s, e)
                    val baseExpr = if (qBody.nonEmpty) {
                      val expr = toBaseExpr(qBody)
                      expr.copy(filter = BinaryClause(q1 = expr.filter, q2 = toTelemetryTypeExistsFilter, op = "and"))
                    } else {
                      BaseExpr(id = "_", dataset = dataset, filter = toTelemetryTypeExistsFilter)
                    }
                    val tagDataType =
                      tagName.map(t => TagDataType(tagName = t, dataType = dataType.getOrElse(STRING_TYPE)))
                    val src = queryEngine
                      .evaluateTagQuery(
                        tagDataType = tagDataType,
                        baseExpr = baseExpr,
                        startDateTime = start,
                        endDateTime = end,
                        customerId = customerId,
                        step = getStepForQueryDuration(start, end),
                        queryId = mkQueryId
                      )
                      .take(limit.getOrElse(1000L)) // enforce default limit of 1K
                      .keepAlive(5.seconds, () => Heartbeat())
                      .map(_.toChunkStreamPart)
                      .watchTermination()(
                        (_, future) =>
                          future.onComplete { _ =>
                            val endTs = System.currentTimeMillis()
                            val ema = updateTotalQueryTime(endTs - startTs)
                            logger
                              .info(
                                s"[$customerId] Tag query took ${(endTs - startTs) / 1000}s (EMA: ${ema / 1000}s) to complete"
                              )
                            recordExecutionTime("tag.query.time", endTs - startTs, s"orgId:$customerId")
                        }
                      )

                    complete {
                      HttpEntity.Chunked(
                        MediaTypes.`text/event-stream`.toContentType,
                        Source.single(Done()).map(_.toChunkStreamPart)
                          .prepend(src)
                          .prepend(SegmentCacheManager.waitUntilScaled("tagsQuery").map(_.toChunkStreamPart))
                      )
                    }
                  }
              }
            }
          }
        }
      }
    }
  }

  private def toTelemetryTypeExistsFilter: Filter = {
    // Creating a synthetic base expr with just the filter clause.
    Filter(
      k = TELEMETRY_TYPE,
      v = List.empty,
      op = EXISTS,
      extracted = false,
      computed = false
    )
  }

  private def graphApi: Route = {
    path("api" / "v1" / "graph") {
      auth { customerId =>
        post {
          entity(as[String]) { qBody: String =>
            {
              logger.info(s"Received request: $qBody")
              parameters("s".?, "e".?) {
                case (s, e) =>
                  if (Commons.isGlobalQueryStack) {
                    makeRegionalCall(
                      customerId = customerId,
                      uri = "/api/v1/graph",
                      params = Map[String, Option[String]]("s" -> s, "e" -> e),
                      qBody = qBody
                    )
                  } else {
                    try {
                      val queryId = mkQueryId
                      val (start: Long, end: Long) = toStartEnd(s, e)
                      val step = getStepForQueryDuration(start, end)

                      val startTs = System.currentTimeMillis()
                      val span = GlobalTracer.get().activeSpan()
                      if (span != null) {
                        span.setTag("query.json", qBody)
                      }
                      val astInput = toASTInput(qBody)
                      val timeSeriesResponseSources = queryEngine
                        .evaluate(
                          astInput,
                          startDateTime = start,
                          endDateTime = end,
                          step,
                          customerId = customerId,
                          queryId = queryId
                        )
                        .watchTermination()(
                          (_, future) =>
                            future.onComplete { _ =>
                              val endTs = System.currentTimeMillis()
                              val ema = updateTotalQueryTime(endTs - startTs)
                              logger.info(
                                s"[$customerId] Aggregate query took ${(endTs - startTs) / 1000}s (EMA: ${ema / 1000}s) to complete"
                              )
                              recordExecutionTime("aggregate.query.time", endTs - startTs, s"orgId:$customerId")
                          }
                        )

                      val logBaseExpressions = astInput.baseExpressions.filter(_._2.isEventTelemetryType)
                      val logExemplarResponseSource =
                        if (logBaseExpressions.isEmpty) Source.empty
                        else {
                          val newAstInput = astInput.copy(
                            baseExpressions = logBaseExpressions
                              .filter(e => e._2.chartOpts.nonEmpty) // Only if exemplar queries are not present.
                              .map(e => e._1 -> e._2.copy(chartOpts = None)),
                            formulae = List()
                          )
                          if (newAstInput.baseExpressions.nonEmpty) {
                            queryEngine
                              .evaluate(
                                newAstInput, // For exemplars, make chartOpts = None
                                startDateTime = start,
                                endDateTime = end,
                                step = step,
                                customerId = customerId,
                                queryId = mkQueryId
                              )
                              .watchTermination()(
                                (_, future) =>
                                  future.onComplete { _ =>
                                    val endTs = System.currentTimeMillis()
                                    val ema = updateTotalQueryTime(endTs - startTs)
                                    logger.info(
                                      s"[$customerId] Exemplar query took ${(endTs - startTs) / 1000}s (EMA: ${ema / 1000}s) to complete"
                                    )
                                    recordExecutionTime("exemplar.query.time", endTs - startTs, s"orgId:$customerId")
                                }
                              )
                          } else Source.empty
                        }

                      val resultSrc = timeSeriesResponseSources.merge(logExemplarResponseSource)
                      val finalSrc =resultSrc.prepend(SegmentCacheManager.waitUntilScaled(queryId))

                      complete {
                        HttpEntity.Chunked(
                          MediaTypes.`text/event-stream`.toContentType,
                          Source.single(Done()).map(_.toChunkStreamPart).prepend(finalSrc.map(_.toChunkStreamPart))
                        )
                      }
                    } catch {
                      case e: Exception =>
                        logger.error("Error responding to request", e)
                        complete {
                          HttpResponse(status = StatusCodes.BadRequest, entity = HttpEntity(e.getMessage))
                        }
                    }
                  }
              }
            }
          }
        }
      }
    }
  }

  private def getStepForQueryDuration(startTime: Long, endTime: Long): Duration = {
    val queryDuration = endTime - startTime
    if (queryDuration <= 1 * 65 * 60 * 1000) { // ~1 hour
      return Duration.parse("PT10S")
    } else if (queryDuration <= 12 * 60 * 60 * 1000) { // ~12 hours
      return Duration.parse("PT1M")
    } else if (queryDuration <= 24 * 60 * 60 * 1000) { // ~24 hours
      return Duration.parse("PT5M")
    } else if (queryDuration <= 3 * 24 * 60 * 60 * 1000) { // ~3 days
      return Duration.parse("PT20M")
    }
    Duration.parse("PT1H")
  }

  private def makeRegionalCall(
    customerId: String,
    uri: String,
    params: Map[String, Option[String]],
    qBody: String
  ): StandardRoute = {
    complete {
      var queryString = params
        .map {
          case (k, Some(v)) => s"$k=$v"
          case _            => ""
        }
        .filter(_.nonEmpty)
        .mkString("&")
      if (queryString.nonEmpty) queryString = s"?$queryString"

      val requests = toRegionalCNames(customerId).map { cName =>
        val regionalQueryApiEndpoint = REGIONAL_QUERY_API_OVERRIDE_END_POINT.getOrElse(s"https://$cName")
        val newUri = s"$regionalQueryApiEndpoint$uri$queryString"
        logger.info(s"Going to call $newUri")
        HttpRequest(
          method = HttpMethods.POST,
          uri = newUri,
          headers = AuthToken.getAuthHeader(customerId),
          entity = HttpEntity(ContentTypes.`application/json`, ByteString(qBody))
        )
      }

      HttpEntity.Chunked(
        MediaTypes.`text/event-stream`.toContentType,
        Source(requests)
          .mapAsync(PARALLELISM) { request =>
            Http().singleRequest(request)
          }
          .flatMapConcat(_.entity.dataBytes)
          .map(ChunkStreamPart(_))
      )
    }
  }

  private def mkQueryId = {
    UUID.randomUUID().toString.substring(0, 7).replace("-", "")
  }

  private def functionSpecs: Route = {
    path("api" / "v1" / "functionSpecs") {
      complete(HttpEntity(ContentTypes.`application/json`, ByteString(Json.encode(ComputeFunction.getSpecs))))
    }
  }

  override def routes: Route = {
    graphApi ~ tagsApi ~ scopeTags ~ functionSpecs ~ healthy ~ cardinalityApi
  }
}
