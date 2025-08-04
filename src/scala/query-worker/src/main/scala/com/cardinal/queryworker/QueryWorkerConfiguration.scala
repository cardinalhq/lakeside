package com.cardinal.queryworker

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.{Bean, Configuration}

@Configuration
class QueryWorkerConfiguration {
  private val logger = LoggerFactory.getLogger(getClass)

  @Bean
  def config(): Config = {
    val config = ConfigFactory.parseResources("application.conf")
    ConfigFactory.load(config)
  }
}
