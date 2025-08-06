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

package com.cardinal.discovery

import com.cardinal.utils.EnvUtils

case class KubernetesClusterConfig(
                             serviceLabels: Map[String, String],
                             namespace: String
                           )
object KubernetesClusterConfig {
  def load(): KubernetesClusterConfig = {
    val rawLabels = EnvUtils.mustFirstEnv(Seq("QUERY_WORKER_LABEL_SELECTOR", "WORKER_POD_LABEL_SELECTOR"))
    val labels = EnvUtils.parseLabels(rawLabels)

    val ns = EnvUtils.mustFirstEnv(Seq("QUERY_WORKER_POD_NAMESPACE", "WORKER_POD_NAMESPACE", "POD_NAMESPACE"))
    KubernetesClusterConfig(labels, ns)
  }
}
