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

package com.cardinal.utils

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpEntity
import akka.stream.scaladsl.Source
import com.cardinal.instrumentation.Metrics.{count, gauge, recordExecutionTime}
import com.cardinal.model._
import com.cardinal.utils.ast.BaseExpr.generateSql
import com.cardinal.utils.ast.SketchInput
import com.typesafe.config.{Config, ConfigFactory}
import org.duckdb.DuckDBConnection
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory

import java.io.File
import java.sql.{ResultSet, Statement}
import java.util.Base64
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

object Commons {
  private val logger = LoggerFactory.getLogger(getClass)
  val CARDINAL_HQ_PREFIX: String = "_cardinalhq"
  private val RESOURCE_PREFIX: String = "resource"
  val PARALLELISM: Int = 10
  val NAME: String = s"$CARDINAL_HQ_PREFIX.name"
  val LOGS: String = "logs"
  val SPANS: String = "spans"
  val METRICS: String = "metrics"
  val METRIC_TYPE_RATE: String = "rate"
  val METRIC_TYPE_COUNTER: String = "count"
  val METRIC_TYPE_GAUGE: String = "gauge"
  val METRIC_TYPE_HISTOGRAM: String = "histogram"
  val TELEMETRY_TYPE: String = s"$CARDINAL_HQ_PREFIX.telemetry_type"
  val BIGINT: String = "BIGINT"
  val TIMESTAMP: String = s"$CARDINAL_HQ_PREFIX.timestamp"
  val STEP_TS: String = "step_ts"
  val VALUE: String = s"$CARDINAL_HQ_PREFIX.value"
  private val SKETCH: String = "sketch"
  val MESSAGE: String = s"$CARDINAL_HQ_PREFIX.message"
  private val LEVEL: String = s"$CARDINAL_HQ_PREFIX.level"
  val EXISTS_REGEX: String = ".*"
  val SERVICE_PORT: Int = 7101
  private val BLOB_STORE_PROVIDER: String = "BLOB_STORE_PROVIDER"
  private val KUBE_NAMESPACE: String = s"$RESOURCE_PREFIX.k8s.namespace.name"
  private val RESOURCE_FILE :String = s"$RESOURCE_PREFIX.file"

  val SERVICE: String = "service"
  private val SERVICE_NAME: String = s"$RESOURCE_PREFIX.service.name"

  private val TRACE_ID: String = s"$CARDINAL_HQ_PREFIX.span_trace_id"
  val SPAN_NAME: String = s"$CARDINAL_HQ_PREFIX.span_name"
  val SPAN_KIND: String = s"$CARDINAL_HQ_PREFIX.span_kind"
  val config: Config = ConfigFactory.load()
  private val FILE_STORE_MOUNT: String = "./db"
  val CONFIG_DB: String = "config"
  val METADATA_DB: String = "metadata"

  val TEN_SECONDS_MILLIS: Int = 10000
  val AUTH_TOKEN_HEADER = "cardinal_token"
  val AUTH_TOKEN_ORG_ID_CLAIM = "org_id"
  val API_KEY_HEADERS: Seq[String] = Seq("x-cardinalhq-api-key", "Api-Key")
  val DEFAULT_CUSTOMER_ID = "cardinalhq.io"
  val CARDINALHQ_ENVIRONMENT: String = sys.env.getOrElse("CARDINALHQ_ENVIRONMENT", "test")
  val CONTROL_PLANE_HOST: String = sys.env.getOrElse("CONTROL_PLANE_HOST",s"control-plane.global.aws.$CARDINALHQ_ENVIRONMENT.cardinalhq.net")
  val REGIONAL_QUERY_API_OVERRIDE_END_POINT : Option[String]  = sys.env.get("REGIONAL_QUERY_API_OVERRIDE_END_POINT")
  val CARDINAL_REGIONAL_DEPLOYMENT_ID = "cardinal-regional"

  val STORAGE_PROFILE_CLOUD_PROVIDER_GOOGLE = "gcp"

  val QUERY_WORKER_CLUSTER = "QUERY_WORKER_CLUSTER"
  val QUERY_WORKER_PORT = 7101
  val QUERY_WORKER_HEARTBEAT = "/api/internal/heartbeat"
  val WORKER_HEARTBEAT_ENDPOINT = "/api/internal/worker/heartbeat"

  val MIN_WORKERS_FOR_QUERY: Int = sys.env.getOrElse("MIN_WORKERS_FOR_QUERY", "5").toInt
  val MIN_WORKERS_PERCENT: Int = sys.env.getOrElse("MIN_WORKERS_PERCENT", "70").toInt
  val MAX_WORKER_WAIT_SECONDS: Int = sys.env.getOrElse("MAX_WORKER_WAIT_SECONDS", "30").toInt
  val WORKER_HEARTBEAT_INTERVAL_SECONDS: Int = sys.env.getOrElse("WORKER_HEARTBEAT_INTERVAL_SECONDS", "30").toInt
  val WORKER_HEARTBEAT_TIMEOUT_SECONDS: Int = sys.env.getOrElse("WORKER_HEARTBEAT_TIMEOUT_SECONDS", "90").toInt

  val DESCENDING = "DESC"

  val INFRA_DIMENSIONS: List[String] =
    List[String](
      KUBE_NAMESPACE,
      SERVICE_NAME,
      RESOURCE_FILE
    )

  val DIMENSIONS_TO_INDEX: List[String] =
    List[String](TELEMETRY_TYPE, NAME, LEVEL, TRACE_ID) ++ INFRA_DIMENSIONS

  val INDEX_FULL_VALUE_DIMENSIONS: List[String] = List(RESOURCE_FILE)

  def pushDownResponseOrdering(pushDownRequest: PushDownRequest): Ordering[Either[DataPoint, SketchInput]] =
    (x: Either[DataPoint, SketchInput], y: Either[DataPoint, SketchInput]) => {
      val xTimestamp = x match {
        case Left(value) => value.timestamp
        case Right(value) => value.timestamp
      }
      val yTimestamp = y match {
        case Left(value) => value.timestamp
        case Right(value) => value.timestamp
      }

      if (pushDownRequest.reverseSort && pushDownRequest.rollupAgg.isEmpty) {
        xTimestamp.compare(yTimestamp) * -1
      } else {
        xTimestamp.compare(yTimestamp)
      }
    }

  def computeFingerprint(fieldName: String, trigram: String): Long = {
    val str = s"$fieldName:$trigram"
    computeHash(str)
  }

  def computeHash(str: String): Long = {
    var i = 0
    var h: Long = 0
    val len = str.length
    while ( {
      i + 3 < len
    }) {
      h = 31 * 31 * 31 * 31 * h + 31 * 31 * 31 * str(i) + 31 * 31 * str(i + 1) + 31 * str(i + 2) + str(i + 3)

      i += 4
    }

    while ( {
      i < len
    }) {
      h = 31 * h + str(i)
      i += 1
    }
    h
  }

  def getDbPath(bucketName: String,
                customerId: String,
                collectorId: String,
                dataset: String,
                dateInt: String,
                hour: String,
                isRemote: Boolean = false): String = {
    var hourStr = hour
    if (hourStr.length == 1) {
      hourStr = s"0$hourStr"
    }
    if (isRemote) {
      val blobStoreProvider = getBlobStoreProvider
      s"$blobStoreProvider://$bucketName/db/$customerId/default/$dateInt/$dataset/$hour"
    } else {
      s"$FILE_STORE_MOUNT/$customerId/default/$dateInt/$dataset/$hour"
    }
  }

  private def getBlobStoreProvider: String = {
    sys.env.getOrElse(BLOB_STORE_PROVIDER, "s3")
  }

  def toDateIntFormat(dt: DateTime): String = {
    var monthOfYear = dt.getMonthOfYear.toString
    var dayOfMonth = dt.getDayOfMonth.toString
    if (monthOfYear.length == 1) {
      monthOfYear = s"0$monthOfYear"
    }
    if (dayOfMonth.length == 1) {
      dayOfMonth = s"0$dayOfMonth"
    }

    s"${dt.getYear}$monthOfYear$dayOfMonth"
  }

  private def getBucketNames (segmentRequests: List[SegmentRequest]) = {
    segmentRequests.map(_.bucketName).toSet
  }

  private def toGlobResultSet(queryId: String, pushDownRequest: PushDownRequest, localParquet: Boolean): (Statement, ResultSet, DuckDBConnection) = {
    var modifiedSql = ""
    try {
      val start = System.currentTimeMillis()
      val segmentRequests = pushDownRequest.segmentRequests
      val readConnection =
        if (localParquet) DuckDbConnectionFactory.getLocalParquetConnection
        else DuckDbConnectionFactory.getSealedReadConnection(bucketNames = getBucketNames(segmentRequests))
      val statement = readConnection.createStatement()

      val paths = segmentRequests
        .map(sr => s"'${toParquetFilePath(localParquet = localParquet, sr)}'")
        .mkString(", ")
      val tableName = s"read_parquet([$paths], union_by_name=True)"
      val describeTableStatement = s"DESCRIBE SELECT * FROM $tableName"
      val describeTableResultSet = statement.executeQuery(describeTableStatement)
      val columnsThatExist = new mutable.HashSet[String]()
      while(describeTableResultSet.next()) {
        val columnName = describeTableResultSet.getString("column_name")
        columnsThatExist += columnName
      }
      describeTableResultSet.close()

      val baseExpr = pushDownRequest.baseExpr
      val nonExistentFields = baseExpr.fieldSet().diff(columnsThatExist)
      val startTs = segmentRequests.map(_.startTs).min
      val endTs = segmentRequests.map(_.endTs).max

      val sql = generateSql(
        baseExpr = baseExpr,
        startTs = startTs,
        endTs = endTs,
        stepInMillis = pushDownRequest.segmentRequests.head.stepInMillis,
        isTagQuery = pushDownRequest.isTagQuery,
        tagDataType = pushDownRequest.tagDataType,
        globalAgg = baseExpr.chartOpts.map(_.aggregation),
        nonExistentFields = nonExistentFields
      )
      modifiedSql = sql.replace("{tableName}", tableName)
      logger.info(s"Modified SQL = $modifiedSql, pushDownRequest isTagQuery=${pushDownRequest.isTagQuery}, tagDataType=${pushDownRequest.tagDataType}")
      val set = statement.executeQuery(modifiedSql)
      val end = System.currentTimeMillis()
      logger.info(
        s"[$queryId][glob/$localParquet/${segmentRequests.size}] toResultSet took ${end - start}ms running $modifiedSql"
      )
      count("glob.queries", 1.0)
      recordExecutionTime("glob.duration", end - start)
      gauge("glob.size", segmentRequests.size)
      (statement, set, readConnection)
    } catch {
      case e: Exception =>
        logger.error(s"[$queryId]Error in reading glob, sql = $modifiedSql ${e.getMessage}")
        (null, null, null)
    }
  }

  private def toParquetFilePath(localParquet: Boolean, segment: SegmentRequest): String = {
    val parquetFilePath = if (localParquet) {
      s"${
        getDbPath(bucketName = segment.bucketName,
          customerId = segment.customerId,
          collectorId = segment.collectorId,
          dataset = segment.dataset,
          dateInt = segment.dateInt,
          hour = segment.hour)
      }/${segment.segmentId}.parquet"
    } else {
      s"${
        getDbPath(bucketName = segment.bucketName,
          customerId = segment.customerId,
          collectorId = segment.collectorId,
          dataset = segment.dataset,
          dateInt = segment.dateInt,
          hour = segment.hour,
          isRemote = segment.sealedStatus)
      }/${segment.segmentId}.parquet"
    }
    parquetFilePath
  }

  private def resultSetToSource(
                                 queryId: String,
                                 resultSet: ResultSet,
                                 statement: Statement,
                                 connection: DuckDBConnection,
                                 closeConnection: Boolean,
                                 segment: SegmentRequest,
                                 pushDownRequest: PushDownRequest,
                                 tagNameCompressionStage: Option[TagNameCompressionStage]
                               )(
                                 implicit as: ActorSystem,
                                 ec: ExecutionContext
                               ): Source[Either[DataPoint, SketchInput], NotUsed] = {
    if (resultSet != null) {
      val start = System.currentTimeMillis()
      val initSrc = Source
        .repeat(NotUsed)
        .takeWhile(_ => resultSet.next())
        .map(
          _ =>
            toDataPoint(
              queryTags = segment.queryTags,
              resultSet,
              tagNameCompressionStage,
              pushDownRequest.processor.flatMap(_.resetValueToField)
            )
        )
        .filter(_.isDefined)
        .map(_.get)
        .via(new PushDownAggregatorStage(pushDownRequest, stepInMillis = segment.stepInMillis))
        .mapConcat {
          case Left(dataPointIterable) => dataPointIterable.map(Left(_)).toList
          case Right(sketchIterable) => sketchIterable.map(Right(_)).toList
        }
        .watchTermination()(
          (_, future) =>
            future.onComplete {
              case Success(_) =>
                logger.info(s"[$queryId] completed streaming segments in ${System.currentTimeMillis() - start}ms")
                resultSet.close()
                statement.close()
                if (closeConnection) connection.close()
              case Failure(ex) =>
                logger.error(s"[$queryId] streaming segments failed in ${System.currentTimeMillis() - start}ms", ex)
                resultSet.close()
                statement.close()
                if (closeConnection) connection.close()
                throw ex
            }
        )
        .mapMaterializedValue(_ => NotUsed)
        .recover {
          case e: Exception =>
            logger.error(s"[$queryId] error in evaluating dataExpr", e)
            throw new RuntimeException(e)
        }

      initSrc
    } else {
      Source.empty
    }
  }

  def evaluatePushDownRequest(
                               queryId: String,
                               localParquet: Boolean = false,
                               pushDownRequest: PushDownRequest
                             )(
                               implicit as: ActorSystem,
                               ec: ExecutionContext
                             ): Source[Either[DataPoint, SketchInput], NotUsed] = {
    implicit val ord: Ordering[Either[DataPoint, SketchInput]] = pushDownResponseOrdering(pushDownRequest)

    // if all local parquet files, or all sealed segments on S3, glob 'em.
    val tagNameCompressionStage = pushDownRequest.processor match {
      case Some(processor) =>
        if (processor.tagNameCompressionEnabled) {
          Some(TagNameCompressionStage())
        } else None
      case None => None
    }
    val globSize = if (localParquet) 10 else 5
    logger.info(s"[$queryId] Using globSize = $globSize")
    val sources = {
      val grouped = pushDownRequest.segmentRequests
        .grouped(globSize)
        .toList

      grouped.map(group => {
        Source
          .single(group)
          .flatMapMerge(
            8, { requests =>
              val (statement, resultSet, connection) = toGlobResultSet(queryId = queryId,
                pushDownRequest = pushDownRequest.copy(segmentRequests = group),
                localParquet = localParquet)

              resultSetToSource(
                queryId = queryId,
                resultSet = resultSet,
                statement = statement,
                connection = connection,
                closeConnection = localParquet,
                segment = requests.head,
                pushDownRequest,
                tagNameCompressionStage
              )
            }
          )
      })
    }
    if (sources.nonEmpty) {
      sources.fold(Source.empty)((s1, s2) => s1.mergeSorted(s2))
    } else {
      // If we have none of the requested segments, respond with timestamp = -1, which will get filtered out on query-api side.
      Source.single(Left(DataPoint(timestamp = -1, value = -1, tags = Map())))
    }
  }

  private def toDataPoint(queryTags: Map[String, Any],
                          resultSet: ResultSet,
                          tagNameCompressionStage: Option[TagNameCompressionStage],
                          resetFieldTo: Option[String]): Option[DataPoint] = {
    val metadata = resultSet.getMetaData
    val colCount = metadata.getColumnCount
    val tags = new mutable.HashMap[String, Any]()
    var index = 1
    val columnName = metadata.getColumnName(index)
    if (columnName != TIMESTAMP && columnName != STEP_TS && columnName != SKETCH) {
      for (i <- 1 to metadata.getColumnCount) {
        val columnName = metadata.getColumnName(i)
        val columnValue = resultSet.getString(i)
        tags += columnName -> columnValue
      }
      NoisyTagsDropper.remove(tags)
      if (tags.nonEmpty) {
        Some(
          DataPoint(
            timestamp = System.currentTimeMillis(),
            tags = tags.result().map(e => e._1 -> String.valueOf(e._2)).toMap,
            value = 0.0
          )
        )
      } else None
    } else {
      val timestamp = resultSet.getLong(index)
      index += 1
      var value = resultSet.getDouble(index)
      index += 1

      for (i <- index to colCount) {
        val tagName = metadata.getColumnName(i)
        val tagValue = resultSet.getString(i)
        if (tagValue != null && String.valueOf(tagValue) != "null" && tagValue.nonEmpty) {
          tags += tagName -> tagValue
        }
        index += 1
      }
      val filtered = tagNameCompressionStage match {
        case Some(tncs) => tncs.eval(tags)
        case None => true
      }
      resetFieldTo.foreach(
        field =>
          tags
            .get(field)
            .foreach(v => value = Try(String.valueOf(v).toDouble).toOption.getOrElse(0.0))
      )
      if (filtered) {
        var tagsMap = tags.map(e => e._1 -> String.valueOf(e._2)).toMap
        if (tagsMap.isEmpty) {
          tagsMap ++= queryTags.map(e => e._1 -> e._2.asInstanceOf[String])
        }
        Some(
          DataPoint(
            timestamp = timestamp,
            tags = tagsMap,
            value = value,
          )
        )
      } else None
    }
  }

  def toZeroFilledHour(hourInt: Int): String = {
    val hourStr = hourInt.toString
    if (hourStr.length == 1) s"0$hourStr" else hourStr
  }

  def toDateIntHour(l: Long): (String, String) = {
    val dt = new DateTime(l, DateTimeZone.UTC)
    (toDateIntFormat(dt), toZeroFilledHour(dt.getHourOfDay))
  }

  def dataPointResponseToSSE(
                              source: Source[Either[DataPoint, SketchInput], NotUsed]
                            ): Source[HttpEntity.ChunkStreamPart, NotUsed] = {
    source
      .map {
        case Left(datapoint) =>
          val map = new mutable.HashMap[String, AnyRef]()
          map.put("timestamp", Long.box(datapoint.timestamp))
          map.put("value", Double.box(datapoint.value))
          map.put("tags", datapoint.tags)
          map.put("type", "exemplar")
          map.toMap
        case Right(dataSketch) =>
          val map = new mutable.HashMap[String, AnyRef]()
          map.put("timestamp", Long.box(dataSketch.timestamp))
          map.put("tags", dataSketch.sketchTags.tags)
          map.put("type", "sketch")
          val sketchRef = dataSketch.sketchTags.sketch match {
            case Left(bytes) => Base64.getEncoder.encodeToString(bytes)
            case Right(m) => m
          }
          map.put("sketchType", dataSketch.sketchTags.sketchType)
          map.put("sketch", sketchRef)
          map.toMap
      }
      .map(dr => GenericSSEPayload(message = dr))
      .map(_.toChunkStreamPart)
      .keepAlive(1.seconds, () => Heartbeat().toChunkStreamPart)
  }

  def toStartEnd(s: Option[String], e: Option[String]): (Long, Long) = {
    val (startTime, endTime) = Strings.timeRange(s.getOrElse("e-1h"), e.getOrElse("now"))
    val start = startTime.toEpochMilli
    val end = endTime.toEpochMilli
    (start, end)
  }

  def toSegmentPathOnS3(bucketName: String, dataset: String, dateInt: String, hour: String, segmentId: String, customerId: String, collectorId: String): String = {
    s"${
      getDbPath(bucketName = bucketName,
        customerId = customerId,
        collectorId = collectorId,
        dataset = dataset,
        dateInt = dateInt,
        hour = hour).replace("./db", "db")
    }/$segmentId.parquet"
  }

  def round(value: Double, precision: Int): Double = {
    val scale = Math.pow(10, precision)
    (math rint (value * scale)) / scale
  }

  def isGlobalQueryStack: Boolean = {
    sys.env.getOrElse("QUERY_STACK", "global") == "global"
  }
}
