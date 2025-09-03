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

package com.cardinal.auth

import com.azure.core.credential.TokenCredential
import com.azure.identity.DefaultAzureCredentialBuilder
import org.slf4j.LoggerFactory
import org.springframework.cache.annotation.{CacheConfig, Cacheable}
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Profile(Array("azure"))
@Component
@CacheConfig(cacheNames = Array("azureCredentials"), cacheManager = "credentialCacheManager")
class AzureCredentialsCache {
  private val logger = LoggerFactory.getLogger(getClass)

  @Cacheable(value = Array("azureCredentials"), key = "#clientId + '_' + #organizationId", sync = true)
  def getCredential(clientId: String, organizationId: String): TokenCredential = {
    logger.debug(s"Getting Azure credential for clientId: $clientId, organizationId: $organizationId")
    
    // Use DefaultAzureCredential which handles the credential chain:
    // 1. Environment variables (AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET)
    // 2. Managed Identity (system-assigned or user-assigned via AZURE_CLIENT_ID)
    // 3. Azure CLI
    // 4. Azure PowerShell
    // 5. IntelliJ IDEA
    // 6. Visual Studio Code
    val credential = new DefaultAzureCredentialBuilder()
      .managedIdentityClientId(clientId) // Use clientId for user-assigned managed identity
      .build()
    
    logger.debug("Created DefaultAzureCredential with managedIdentityClientId: {}", clientId)
    credential
  }

  def getDefaultCredential: TokenCredential = {
    logger.debug("Creating default Azure credential chain")
    new DefaultAzureCredentialBuilder().build()
  }
  
  /**
   * Get Azure credentials for DuckDB (similar to AWS credentials cache pattern)
   * Cache the environment variable lookup/validation
   */
  @Cacheable(
    value = Array("azureDuckDbCredentials"),
    key = "#storageAccount + '_' + #organizationId",
    sync = true
  )
  def getDuckDbCredentials(storageAccount: String, organizationId: String): Option[(String, String, String)] = {
    logger.debug(s"Loading Azure credentials for DuckDB - storageAccount: $storageAccount, org: $organizationId")
    
    val clientId = sys.env.get("AZURE_CLIENT_ID")
    val clientSecret = sys.env.get("AZURE_CLIENT_SECRET")
    val tenantId = sys.env.get("AZURE_TENANT_ID")
    
    (clientId, clientSecret, tenantId) match {
      case (Some(id), Some(secret), Some(tenant)) =>
        logger.debug(s"✓ Found cached Azure DuckDB credentials for storage account: $storageAccount")
        Some((id, secret, tenant))
      case _ =>
        logger.warn(s"Missing Azure environment variables for storage account: $storageAccount")
        logger.warn(s"  AZURE_CLIENT_ID: ${clientId.isDefined}")
        logger.warn(s"  AZURE_CLIENT_SECRET: ${clientSecret.isDefined}")
        logger.warn(s"  AZURE_TENANT_ID: ${tenantId.isDefined}")
        None
    }
  }
}