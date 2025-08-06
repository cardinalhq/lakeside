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

package com.cardinal.directives

import com.cardinal.dbutils.DBDataSources.getConfigSource
import com.cardinal.directives.ApiKeyCache.{apiKeyByCustomerId, customerIdByApiKey}
import org.slf4j.LoggerFactory

import java.sql.{Connection, Statement}
import java.util.concurrent.atomic.AtomicReference

object ApiKeyCache {
  private val logger = LoggerFactory.getLogger(getClass)
  private val customerIdByApiKey = new AtomicReference[Map[String, String]](Map.empty)
  private val apiKeyByCustomerId = new AtomicReference[Map[String, String]](Map.empty)

  def getCustomerId(apiKey: String): Option[String] = {
    val map = customerIdByApiKey.get()
    if (map == null) None
    else {
      map.get(apiKey)
    }
  }

  def getApiKey(customerId: String): Option[String] = {
    val map = apiKeyByCustomerId.get()
    if (map == null) None
    else {
      map.get(customerId)
    }
  }

  def getApiKeyByCustomerIdMap: Map[String, String] = {
    apiKeyByCustomerId.get()
  }

  def setApiKeyByCustomerIdMap(apiKeyByCustomerIdMap: Map[String, String]): Unit = {
    if (apiKeyByCustomerIdMap.nonEmpty) {
      apiKeyByCustomerId.set(apiKeyByCustomerIdMap)
      val customerIdByApiKeyMap = apiKeyByCustomerIdMap.map(_.swap)
      customerIdByApiKey.set(customerIdByApiKeyMap)
    } else {
      logger.error("API Key map is empty")
    }
  }
}

class ApiKeyCache {
  private val logger = LoggerFactory.getLogger(getClass)

  private val sql = "SELECT organization_id, api_key FROM c_organization_api_keys WHERE enabled = true;"

  def start(): Unit = {
    new Thread(() => {
      while (true) {
        pollDb()
        Thread.sleep(60000)
      }
    }).start()

    while (customerIdByApiKey.get() == null) {
      logger.info("Waiting for apiKey cache to be populated")
      Thread.sleep(1000)
    }
  }

  def pollDb(): Unit = {
    var connection: Connection = null
    var statement: Statement = null
    val orgIdsByApiKey = Map.newBuilder[String, String]
    val apiKeyByOrgId = Map.newBuilder[String, String]
    try {
      connection = getConfigSource.getConnection
      statement = connection.createStatement()
      val resultSet = statement.executeQuery(sql)
      while (resultSet.next()) {
        val organizationId = resultSet.getString("organization_id")
        val apiKey = resultSet.getString("api_key")
        orgIdsByApiKey += apiKey        -> organizationId
        apiKeyByOrgId += organizationId -> apiKey
      }
      customerIdByApiKey.set(orgIdsByApiKey.result())
      apiKeyByCustomerId.set(apiKeyByOrgId.result())
      resultSet.close()
    } catch {
      case e: Exception =>
        logger.error(s"Error in api key poll ${e.getMessage}", e)
    } finally {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }
}
