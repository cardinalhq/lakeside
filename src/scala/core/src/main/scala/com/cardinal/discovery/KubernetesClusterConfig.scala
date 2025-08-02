package com.cardinal.discovery

import com.cardinal.utils.EnvUtils

case class KubernetesClusterConfig(
                             serviceLabels: Map[String, String],
                             namespace: String
                           )
object KubernetesClusterConfig {
  def load(): KubernetesClusterConfig = {
    val rawLabels = EnvUtils.mustGet("WORKER_POD_LABEL_SELECTOR")
    val labels = EnvUtils.parseLabels(rawLabels)

    val ns = EnvUtils.mustFirstEnv(Seq("WORKER_POD_NAMESPACE", "POD_NAMESPACE"))
    KubernetesClusterConfig(labels, ns)
  }
}
