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

package com.cardinal.utils.ast.queries

import com.cardinal.model.query.common.SegmentInfo
import com.cardinal.utils.Commons
import com.cardinal.utils.Commons._
import com.netflix.atlas.json.Json
import com.netflix.spectator.atlas.impl.Query
import com.sun.jna.{Library, Native}
import org.slf4j.LoggerFactory

import java.nio.file.Paths
import scala.collection.mutable

trait RegexpInterface extends Library {
  def processQuery(str: String): String
}

case class TrigramQuery(Op: Int, Trigram: Set[String], Sub: Option[List[TrigramQuery]]) {
  var fieldName: String = _
  var fieldValue: String = _
  val fingerprints: mutable.HashSet[Long] = new mutable.HashSet[Long]()
}

object NLPUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  private val regexp: RegexpInterface = try {
    Native.load(
      Paths.get("/app/libs/lib-trigram.so").toFile.getAbsolutePath,
      classOf[RegexpInterface]
    )
  } catch {
    case e: Exception =>
      logger.error("Error loading lib", e)
      throw new RuntimeException(e)
  }

  private def toTrigramQuery(str: String): TrigramQuery = {
    if (str == EXISTS_REGEX) TrigramQuery(2, Set(str), Sub = None)
    else {
      val processed = regexp.processQuery(str)
      val result = Json.decode[TrigramQuery](processed)
      if (result.Trigram == null && result.Sub.isEmpty) {
        TrigramQuery(0, Set(str), Sub = None)
      } else {
        result
      }
    }
  }

  def addFingerprints(trigramQuery: TrigramQuery, allFps: mutable.HashSet[Long]): Unit = {
    if (trigramQuery.Trigram != null && trigramQuery.Trigram.nonEmpty) {
      if (Commons.INDEX_FULL_VALUE_DIMENSIONS.contains(trigramQuery.fieldName)) {
        val fp = computeFingerprint(
          fieldName = trigramQuery.fieldName,
          trigram = trigramQuery.fieldValue
        )
        allFps += fp
        trigramQuery.fingerprints += fp
      } else {
        trigramQuery.Trigram.foreach(trigram => {
          val fp = computeFingerprint(
            fieldName = trigramQuery.fieldName,
            trigram = trigram
          )
          allFps += fp
          trigramQuery.fingerprints += fp
        })
      }
    }
    trigramQuery.Sub.getOrElse(List()).foreach(t => addFingerprints(t, allFps))
  }

  def toTrigramQuery(query: Query, dataset: String): Option[TrigramQuery] = {
    query match {
      case AndQuery(q1, q2) =>
        Some(
          TrigramQuery(
            Op = 2,
            Trigram = Set[String](),
            Sub = {
              val sub = (toTrigramQuery(q1, dataset), toTrigramQuery(q2, dataset)) match {
                case (Some(t1), None) => List(t1)
                case (None, Some(t2)) => List(t2)
                case (Some(t1), Some(t2)) =>
                  List(t1, t2)
                case (None, None) => throw new IllegalArgumentException("One of two queries needs to be present!")
              }
              Some(sub)
            }
          )
        )
      case OrQuery(q1, q2) =>
        Some(
          TrigramQuery(
            Op = 3,
            Trigram = Set[String](),
            Sub = Some((toTrigramQuery(q1, dataset) ++ toTrigramQuery(q2, dataset)).toList)
          )
        )
      case HasQuery(k)         => toTrigramQuery(k, EXISTS_REGEX)
      case NotQuery(_)         => None
      case EqualsQuery(k, v)   => mkTrigramQueryConditionalOnDataset(dataset, k, toTrigramValue(v))
      case RegexQuery(k, v)    => mkTrigramQueryConditionalOnDataset(dataset, k, v)
      case ContainsQuery(k, v) => mkTrigramQueryConditionalOnDataset(dataset, k, s".*$v.*")
      case InQuery(k, vs) =>
        Some(
          TrigramQuery(
            Op = 3,
            Trigram = Set[String](),
            Sub = Some(vs.map(v => mkTrigramQueryConditionalOnDataset(dataset, k, toTrigramValue(v)).get).toList)
          )
        )
      case _ => None
    }
  }

  def toTrigramValue(v: String): String = {
    v
  }

  private val INDEXED_DIMENSIONS = DIMENSIONS_TO_INDEX ++ INFRA_DIMENSIONS.toSet
  private def mkTrigramQueryConditionalOnDataset(dataset: String, k: String, v: String): Option[TrigramQuery] = {
    if (dataset != METRICS && k == NAME) toTrigramQuery(TELEMETRY_TYPE, dataset)
    else if (!INDEXED_DIMENSIONS.contains(k)) {
      toTrigramQuery(k, EXISTS_REGEX)
    } else {
      toTrigramQuery(k, v)
    }
  }

  private def toTrigramQuery(k: String, v: String): Option[TrigramQuery] = {
    val q = toTrigramQuery(v)
    q.fieldName = k
    q.fieldValue = v
    Some(q)
  }

  def computeSegmentIds(trigramQuery: TrigramQuery, fpToSegmentIds: Map[Long, Set[SegmentInfo]]): Set[SegmentInfo] = {
    trigramQuery.Sub match {
      case Some(trigramQueries) =>
        trigramQuery.Op match {
          case 0 => fpToSegmentIds.flatMap(_._2).toSet // everything matches
          case 1 => Set.empty // nothing matches
          case 2 => // and
            val sets = trigramQueries.map(t => computeSegmentIds(t, fpToSegmentIds))
            val result = sets.reduce((s1, s2) => s1.intersect(s2))
            result
          case 3 => // or
            trigramQueries.map(t => computeSegmentIds(t, fpToSegmentIds)).reduce((s1, s2) => s1.union(s2))
        }
      case None =>
        trigramQuery.Op match {
          case 0 => fpToSegmentIds.flatMap(_._2).toSet
          case 1 => Set.empty
          case 2 =>
            if (fpToSegmentIds.values.isEmpty) Set[SegmentInfo]()
            else {
              val sets = trigramQuery.fingerprints.toList.map(fp => fpToSegmentIds.getOrElse(fp, Set()))
              val result = sets.reduce((s1, s2) => s1.intersect(s2))
              result
            }
          case 3 =>
            if (fpToSegmentIds.values.isEmpty) Set[SegmentInfo]()
            else {
              val sets = trigramQuery.fingerprints.toList.map(fp => fpToSegmentIds.getOrElse(fp, Set()))
              sets.reduce((s1, s2) => s1.union(s2))
            }
        }
    }
  }
}
