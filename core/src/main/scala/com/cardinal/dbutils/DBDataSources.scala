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

package com.cardinal.dbutils

import com.cardinal.utils.Commons.METADATA_DB
import com.cardinal.utils.EnvUtils.firstEnv
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

object DBDataSources {
  // Environment / default for the "LRDB" (metadata) database
  private val lrdbHost     = firstEnv(Seq("LRDB_HOST", "POSTGRES_HOST"))
  private val lrdbPort     = firstEnv(Seq("LRDB_PORT"), "5432")
  private val lrdbUser     = firstEnv(Seq("LRDB_USER", "POSTGRES_USER"))
  private val lrdbPassword = firstEnv(Seq("LRDB_PASSWORD", "POSTGRES_PASSWORD"))
  private val lrdbDatabase = firstEnv(Seq("LRDB_DBNAME"), METADATA_DB)
  private val lrdbSslMode  = firstEnv(Seq("LRDB_SSLMODE"))

  // Environment / default for the "config" database
  private val configHost     = firstEnv(Seq("CONFIGDB_HOST", "POSTGRES_HOST"))
  private val configPort     = firstEnv(Seq("CONFIGDB_PORT"), "5432")
  private val configUser     = firstEnv(Seq("CONFIGDB_USER", "POSTGRES_USER"))
  private val configPassword = firstEnv(Seq("CONFIGDB_PASSWORD", "POSTGRES_PASSWORD"))
  private val configDatabase = firstEnv(Seq("CONFIGDB_DBNAME"), "config")
  private val configSslMode  = firstEnv(Seq("CONFIGDB_SSLMODE"))

  // Load the PostgreSQL driver
  Class.forName("org.postgresql.Driver")

  private def makeDataSource(
                              host: String,
                              port: String,
                              dbName: String,
                              user: String,
                              pass: String,
                              sslMode: String,
                              maxPoolSize: Int
                            ): HikariDataSource = {
    val cfg = new HikariConfig()
    val baseUrl = s"jdbc:postgresql://$host:$port/$dbName"
    val jdbcUrl =
      if (sslMode.trim.nonEmpty) s"$baseUrl?sslmode=$sslMode"
      else baseUrl
    cfg.setJdbcUrl(jdbcUrl)
    cfg.setUsername(user)
    cfg.setPassword(pass)
    cfg.setMaximumPoolSize(maxPoolSize)
    new HikariDataSource(cfg)
  }

  private lazy val lrdbDataSource: HikariDataSource =
    makeDataSource(
      host = lrdbHost,
      port = lrdbPort,
      dbName = lrdbDatabase,
      user = lrdbUser,
      pass = lrdbPassword,
      sslMode = lrdbSslMode,
      maxPoolSize = 20
    )

  def getLRDBSource: HikariDataSource = lrdbDataSource

  private lazy val configDataSource: HikariDataSource =
    makeDataSource(
      host = configHost,
      port = configPort,
      dbName = configDatabase,
      user = configUser,
      pass = configPassword,
      sslMode = configSslMode,
      maxPoolSize = 10
    )

  def getConfigSource: HikariDataSource = configDataSource
}
