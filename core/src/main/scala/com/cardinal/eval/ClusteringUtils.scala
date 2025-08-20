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

package com.cardinal.eval

import scala.collection.mutable
import scala.util.Random

object SimpleKMeans {

  private def euclideanDistance(a: Array[Double], b: Array[Double]): Double = {
    math.sqrt(a.zip(b).map { case (x, y) => math.pow(x - y, 2) }.sum)
  }

  private def findClosestCentroid(point: Array[Double], centroids: Array[Array[Double]]): Int = {
    centroids.zipWithIndex
      .map {
        case (centroid, idx) =>
          (idx, euclideanDistance(point, centroid))
      }
      .minBy(_._2)
      ._1
  }

  private def computeNewCentroid(cluster: Seq[Array[Double]]): Array[Double] = {
    val numPoints = cluster.size
    val summedPoints = cluster.reduce((a, b) => a.zip(b).map { case (x, y) => x + y })
    summedPoints.map(_ / numPoints)
  }

  def kmeans(data: Array[Array[Double]], k: Int, maxIterations: Int = 100): Array[Int] = {
    val random = new Random()
    var centroids = Array.fill(k)(data(random.nextInt(data.length)))

    for (_ <- 0 until maxIterations) {
      val clusters = data.groupBy(findClosestCentroid(_, centroids))

      centroids = clusters.map {
        case (_, cluster) =>
          computeNewCentroid(cluster)
      }.toArray
    }

    data.map(findClosestCentroid(_, centroids))
  }
}

object ClusteringUtils {

  private def padVector(vec: Array[Double], length: Int): Array[Double] = {
    if (vec.length >= length) vec
    else vec ++ Array.fill(length - vec.length)(0.0)
  }

  // Convert a string into a feature vector.
  private def jaccardSimilarity(str1: String, str2: String, n: Int = 2): Double = {
    val ngrams1 = str1.sliding(n).toSet
    val ngrams2 = str2.sliding(n).toSet

    val intersectionSize = (ngrams1 & ngrams2).size.toDouble
    val unionSize = (ngrams1 | ngrams2).size.toDouble

    if (unionSize == 0) 0 else intersectionSize / unionSize
  }

  private def generateFeatureVector(metric: String, allMetrics: Array[String]): Array[Double] = {
    allMetrics.map(otherMetric => jaccardSimilarity(metric, otherMetric))
  }

  private val COMMON_SUFFIXES: Set[String] =
    Set[String]("95percentile", "max", "median", "count", "avg", "50p", "75p", "95p", "99p", "sum", "total")

  def clustered(metricNames: List[String], minK: Int, maxK: Int, maxClusterSize: Int): List[List[String]] = {
    if (metricNames.size <= 5) {
      List(metricNames)
    } else {
      val metricNameToSuffixes = new mutable.HashMap[String, mutable.HashSet[String]]
      val metricNamesWithSuffixesRemoved = Set.newBuilder[String]
      metricNames.foreach { metricName =>
        val commonSuffix = COMMON_SUFFIXES.find(cs => metricName.endsWith(s".$cs"))
        commonSuffix match {
          case Some(cs) =>
            val newMetricName = metricName.replace(cs, "")
            metricNamesWithSuffixesRemoved += newMetricName
            val suffixes = metricNameToSuffixes.getOrElseUpdate(newMetricName, new mutable.HashSet[String]())
            suffixes.add(cs)
          case None =>
            metricNamesWithSuffixesRemoved += metricName
        }
      }
      var answer: List[List[String]] = List.empty
      var requirementMet: Boolean = false
      for (i <- minK to maxK) {
        if (!requirementMet) {
          answer = getBatches(metricNames = metricNamesWithSuffixesRemoved.result().toList, k = i)
          requirementMet = answer.forall(_.size < maxClusterSize)
        }
      }
      answer.map { cluster =>
        cluster
          .map { m =>
            metricNameToSuffixes.get(m) match {
              case Some(suffixSet) =>
                if (suffixSet.contains("95percentile")) s"${m}95percentile"
                else if (suffixSet.contains("95p")) s"${m}95p"
                else if (suffixSet.contains("sum")) s"${m}sum"
                else if (suffixSet.contains("count")) s"${m}count"
                else if (suffixSet.contains("total")) s"${m}total"
                else ""
              case None => m
            }
          }
          .filter(_.nonEmpty)
      }
    }
  }

  private def getBatches(metricNames: List[String], k: Int): List[List[String]] = {
    val featureVectors = metricNames.map(metric => generateFeatureVector(metric, metricNames.toArray))

    val maxLength = featureVectors.map(_.length).max
    val paddedFeatureVectors = featureVectors.map(v => padVector(v, maxLength)).toArray

    val clusterAssignments = SimpleKMeans.kmeans(paddedFeatureVectors, k)

    val groupedMetrics = metricNames
      .zip(clusterAssignments)
      .groupBy(_._2)
      .map { case (key, value) => key -> value.map(_._1) }

    val stringClusters = List.newBuilder[List[String]]

    for ((_, metricsInCluster) <- groupedMetrics) {
      stringClusters += metricsInCluster
    }

    stringClusters.result()
  }
}

object TestClusteringUtils extends App {

  val metrics: Array[String] = Array(
    "gauge:ingestion.num_column_adds",
    "gauge:ingestion.segmentCreationTime.max",
    "gauge:ingestion.segmentCreationTime.95percentile",
    "gauge:ingestion.buffer_flow_upstream_delay.median",
    "rate:ingestion.buffer_flow_upstream_delay.count",
    "gauge:ingestion.row_append_time.95percentile",
    "rate:ingestion.evalQueue",
    "gauge:ingestion.segmentCreationTime.median",
    "rate:ingestion.row_append_time.count",
    "rate:ingestion.num_events",
    "gauge:ingestion.row_append_time.median",
    "gauge:ingestion.index_append_calc_time.median",
    "gauge:ingestion.ingestionLag.max",
    "rate:ingestion.batch_processing_time.count",
    "gauge:ingestion.batch_processing_time.max",
    "rate:ingestion.numEvalForwards",
    "rate:ingestion.ingestionLag.count",
    "gauge:ingestion.encode_flow_upstream_delay.max",
    "rate:ingestion.encode_flow_upstream_delay.count",
    "rate:ingestion.encode_flow_downstream_delay.count",
    "gauge:ingestion.encode_flow_downstream_delay.avg",
    "gauge:ingestion.buffer_flow_upstream_delay.avg",
    "gauge:ingestion.ingestionLag.median",
    "gauge:ingestion.batch_processing_time.avg",
    "rate:ingestion.instruction_flow_downstream_delay.count",
    "gauge:ingestion.buffer_flow_downstream_delay.max",
    "rate:ingestion.localQueue",
    "rate:ingestion.spanRouterQueue",
    "gauge:ingestion.ingestionLag.95percentile",
    "rate:ingestion.instruction_flow_upstream_delay.count",
    "gauge:ingestion.ingestionLag.avg",
    "gauge:ingestion.buffer_flow_downstream_delay.avg",
    "gauge:ingestion.local_store_flow_downstream_delay.max",
    "gauge:ingestion.local_store_flow_downstream_delay.95percentile",
    "gauge:ingestion.local_store_flow_downstream_delay.avg",
    "rate:ingestion.local_store_flow_downstream_delay.count",
    "gauge:ingestion.instruction_flow_upstream_delay.95percentile",
    "rate:ingestion.logs.ingest.rps",
    "gauge:ingestion.instruction_flow_upstream_delay.median",
    "gauge:ingestion.instruction_flow_upstream_delay.avg",
    "gauge:ingestion.local_store_flow_upstream_delay.median",
    "gauge:ingestion.local_store_flow_upstream_delay.max",
    "rate:ingestion.spans.buffer.processed",
    "gauge:ingestion.buffer_flow_upstream_delay.max",
    "gauge:ingestion.instruction_flow_upstream_delay.max",
    "rate:ingestion.local_store_flow_upstream_delay.count",
    "gauge:ingestion.local_store_flow_upstream_delay.avg",
    "gauge:ingestion.index_append_calc_time.avg",
    "gauge:ingestion.ingest.buffer.size",
    "gauge:ingestion.encode_flow_downstream_delay.median",
    "gauge:ingestion.encode_flow_downstream_delay.95percentile",
    "gauge:ingestion.local_store_flow_downstream_delay.median",
    "rate:ingestion.metrics.ingest.rps",
    "gauge:ingestion.row_append_time.max",
    "gauge:ingestion.segmentCreationTime.avg",
    "rate:ingestion.eventsQueue",
    "gauge:ingestion.row_append_time.avg",
    "rate:ingestion.buffer_flow_downstream_delay.count",
    "rate:ingestion.spanBufferQueue",
    "gauge:ingestion.encode_flow_upstream_delay.median",
    "gauge:ingestion.encode_flow_upstream_delay.95percentile",
    "rate:ingestion.spans.ingest.rps",
    "gauge:ingestion.local_store_flow_upstream_delay.95percentile",
    "gauge:ingestion.encode_flow_upstream_delay.avg",
    "gauge:ingestion.instruction_flow_downstream_delay.max",
    "gauge:ingestion.instruction_flow_downstream_delay.median",
    "rate:ingestion.indexQueue",
    "rate:ingestion.samples_per_second",
    "gauge:ingestion.batch_processing_time.95percentile",
    "gauge:ingestion.batch_processing_time.median",
    "gauge:ingestion.internal_buffer_spans_upstream_delay.median",
    "rate:ingestion.internal_buffer_spans_upstream_delay.count",
    "gauge:ingestion.buffer_flow_downstream_delay.95percentile",
    "gauge:ingestion.internal_buffer_spans_upstream_delay.max",
    "gauge:ingestion.internal_buffer_spans_downstream_delay.median",
    "gauge:ingestion.internal_buffer_spans_upstream_delay.avg",
    "gauge:ingestion.internal_buffer_spans_downstream_delay.avg",
    "rate:ingestion.internal_buffer_spans_downstream_delay.count",
    "gauge:ingestion.internal_buffer_spans_downstream_delay.95percentile",
    "rate:ingestion.segmentCreationTime.count",
    "rate:ingestion.dataRouter",
    "gauge:ingestion.index_append_calc_time.max",
    "gauge:ingestion.index_append_calc_time.95percentile",
    "gauge:ingestion.encode_flow_downstream_delay.max",
    "rate:ingestion.index_append_calc_time.count",
    "gauge:ingestion.batch_size",
    "gauge:ingestion.instruction_flow_downstream_delay.95percentile",
    "rate:ingestion.clientSendQueue",
    "gauge:ingestion.buffer_flow_upstream_delay.95percentile",
    "gauge:ingestion.buffer_flow_downstream_delay.median",
    "gauge:ingestion.instruction_flow_downstream_delay.avg",
    "gauge:ingestion.internal_buffer_spans_upstream_delay.95percentile",
    "gauge:ingestion.internal_buffer_spans_downstream_delay.max"
  )

  private val batches = ClusteringUtils.clustered(metrics.toList, 2, 6, 15)
  batches.zipWithIndex.foreach { batch =>
    val (b, index) = batch
    println(s"Cluster $index:\n${b.mkString("\n")}\n\n")
  }
}
