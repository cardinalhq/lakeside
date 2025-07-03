package com.cardinal.dbutils

import com.cardinal.utils.Commons.{CONFIG_DB, METADATA_DB}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

object DBDataSources {
  private val postgresHost = sys.env.getOrElse("POSTGRES_HOST", "")
  private val username     = sys.env.getOrElse("POSTGRES_USER", "")
  private val password     = sys.env.getOrElse("POSTGRES_PASSWORD", "")

  Class.forName("org.postgresql.Driver")

  private def makeDataSource(dbName: String, maxPoolSize: Int): HikariDataSource = {
    val cfg = new HikariConfig()
    cfg.setJdbcUrl(s"jdbc:postgresql://$postgresHost:5432/$dbName")
    cfg.setUsername(username)
    cfg.setPassword(password)
    cfg.setMaximumPoolSize(maxPoolSize)
    new HikariDataSource(cfg)
  }

  private lazy val metadataDataSource: HikariDataSource = makeDataSource(METADATA_DB, 20)

  def getMetadataSource: HikariDataSource = metadataDataSource
}
