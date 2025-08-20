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

package com.cardinal.model

import com.cardinal.model.DateIntHours.toDateHours
import com.cardinal.model.FingerprintRequest.logger
import com.cardinal.utils.Commons.{computeFingerprint, INDEX_FULL_VALUE_DIMENSIONS}
import com.cardinal.utils.ast.queries.NLPUtils.{addFingerprints, toTrigramQuery, toTrigramValue}
import com.cardinal.utils.ast.queries.TrigramQuery
import com.cardinal.utils.ast.{ASTUtils, BaseExpr}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.netflix.atlas.json.Json
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object FingerprintRequest {
  private val logger = LoggerFactory.getLogger(getClass)
}

case class FingerprintRequest(
  queryId: String,
  startTs: Long,
  endTs: Long,
  baseExpressions: List[BaseExpr],
  customerId: String,
  frequency: Long,
  var skipSegmentResolution: Boolean = false,
) {
  def toJson: String = {
    Json.encode(
      Map[String, AnyRef](
        "startTs"         -> Long.box(startTs),
        "endTs"           -> Long.box(endTs),
        "customerId"      -> customerId,
        "baseExpressions" -> baseExpressions.map(_.toJsonObj),
        "frequency"       -> Long.box(frequency)
      )
    )
  }

  @JsonIgnore
  private lazy val dateHours: Seq[DateIntHours] =
    toDateHours(new DateTime(startTs).withZone(DateTimeZone.UTC), new DateTime(endTs).withZone(DateTimeZone.UTC))

  @JsonIgnore
  lazy val hoursByDateInt: Map[String, Seq[DateIntHours]] = dateHours.groupBy(_.dateInt)

  @JsonIgnore
  lazy val trigramsQueriesByBaseExpr: Map[BaseExpr, (TrigramQuery, Set[Long])] = try {
    baseExpressions
      .map { baseExpr =>
        baseExpr -> {
          val allFps = new mutable.HashSet[Long]()
          val filter = baseExpr.filter
          val queryTags = baseExpr.queryTags()
          val astQuery = ASTUtils.toQuery(filter, skipExtractedComputed = true)
          val tqOpt = toTrigramQuery(astQuery, dataset = baseExpr.dataset)

          val fullValueDimensions = INDEX_FULL_VALUE_DIMENSIONS.toSet.intersect(queryTags.keySet)
          if (fullValueDimensions.nonEmpty) {
            fullValueDimensions.foreach {
              dimensionName =>
                queryTags(dimensionName) match {
                  case fieldValue: String =>
                    val fp = computeFingerprint(
                      fieldName = dimensionName,
                      trigram = toTrigramValue(fieldValue)
                    )
                    allFps.add(fp)
                  case fieldValues: List[String] =>
                    fieldValues.foreach { fieldValue =>
                      val fp = computeFingerprint(
                        fieldName = dimensionName,
                        trigram = toTrigramValue(fieldValue)
                      )
                      allFps.add(fp)
                    }
                }
            }
            if(allFps.nonEmpty) {
              skipSegmentResolution = true
            }
          } else {
            tqOpt.foreach { tq =>
              addFingerprints(tq, allFps)
            }
          }
          (tqOpt, allFps.toSet)
        }
      }
      .filter(e => e._2._1.isDefined)
      .map(e => e._1 -> (e._2._1.get, e._2._2))
      .toMap
  }
  catch {
    case e: Exception =>
      logger.error("Error while converting to trigrams", e)
      Map.empty[BaseExpr, (TrigramQuery, Set[Long])]
  }
}
