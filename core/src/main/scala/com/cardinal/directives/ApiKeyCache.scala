package com.cardinal.directives

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{Caffeine, CacheLoader, LoadingCache}
import com.cardinal.dbutils.DBDataSources.getConfigSource
import org.slf4j.LoggerFactory

object ApiKeyCache {
  private val logger = LoggerFactory.getLogger(getClass)
  private val sql =
    """
      |SELECT organization_id
      |  FROM c_organization_api_keys
      | WHERE api_key = ?
      |   AND enabled = true
    """.stripMargin

  private val cache: LoadingCache[String, Option[String]] =
    Caffeine.newBuilder()
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(new CacheLoader[String, Option[String]] {
        override def load(apiKey: String): Option[String] = {
          var conn: Connection = null
          var ps: PreparedStatement = null
          var rs: ResultSet = null
          try {
            conn = getConfigSource.getConnection
            ps   = conn.prepareStatement(sql)
            ps.setString(1, apiKey)
            rs = ps.executeQuery()
            if (rs.next()) Option(rs.getString("organization_id")) else None
          } catch {
            case e: Exception =>
              logger.error(s"Error loading API key=$apiKey", e)
              None
          } finally {
            if (rs != null) rs.close()
            if (ps != null) ps.close()
            if (conn != null) conn.close()
          }
        }
      })

  /**
   * Returns Some(customerId) if found (and caches it),
   * or None if missing or on error.
   */
  def getCustomerId(apiKey: String): Option[String] = cache.get(apiKey)
}
