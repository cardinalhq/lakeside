package com.cardinal.queryworker

import com.cardinal.utils.Commons.isRunningInKubernetes
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.{Bean, Configuration}

import scala.util.Try

@Configuration
class QueryWorkerConfiguration {
  private val logger = LoggerFactory.getLogger(getClass)

  @Bean
  def config(): Config = {
    val appConfig = ConfigFactory.parseResources("application.conf")
    // So we can provide some overrides when running the app locally
    val config = if (!isRunningInKubernetes) {
      Try(ConfigFactory.parseResources("application-laptop.conf")).toOption
        .map(localConf => localConf.withFallback(appConfig))
        .getOrElse(appConfig)
    } else {
      logger.info("Running on kubernetes, using discovery config")
      appConfig
    }

    ConfigFactory.load(config)
  }
}
