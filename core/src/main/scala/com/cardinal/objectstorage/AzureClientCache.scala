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

package com.cardinal.objectstorage

import com.azure.storage.blob.{BlobClient, BlobContainerClient, BlobServiceClient, BlobServiceClientBuilder}
import com.cardinal.auth.AzureCredentialsCache
import com.cardinal.config.StorageProfileCache
import org.slf4j.LoggerFactory
import org.springframework.cache.annotation.{CacheConfig, Cacheable}
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Profile(Array("azure"))
@Component
@CacheConfig(cacheNames = Array("azureClients"), cacheManager = "clientCacheManager")
class AzureClientCache(
  storageProfileCache: StorageProfileCache,
  azureCredentialsCache: AzureCredentialsCache
) {
  private val logger = LoggerFactory.getLogger(getClass)

  @Cacheable(value = Array("azureClients"), key = "#containerName", sync = true)
  def getContainerClient(containerName: String): BlobContainerClient = {
    try {
      val storageProfile = storageProfileCache.getStorageProfile(containerName)
        .getOrElse(throw new RuntimeException(s"No storage profile found for container: $containerName"))
      
      logger.debug(s"Found storage profile ${storageProfile.storageProfileId} for container $containerName in region ${storageProfile.region}")
      
      val serviceClient = createBlobServiceClient(storageProfile)
      serviceClient.getBlobContainerClient(containerName)
    } catch {
      case e: Exception =>
        logger.error(s"Error creating Azure container client for container $containerName", e)
        throw e
    }
  }

  def getBlobClient(containerName: String, blobName: String): BlobClient = {
    getContainerClient(containerName).getBlobClient(blobName)
  }

  private def createBlobServiceClient(profile: com.cardinal.model.StorageProfile): BlobServiceClient = {
    // Get Azure credential using the role (clientId) and organizationId from storage profile
    val credential = azureCredentialsCache.getCredential(profile.role, profile.organizationId)
    
    // Extract storage account from endpoint or construct default endpoint
    val endpoint = profile.endpoint.getOrElse {
      val storageAccount = extractStorageAccountName(profile)
      s"https://$storageAccount.blob.core.windows.net"
    }
    
    logger.debug(s"Creating Azure Blob Service client for endpoint: $endpoint")
    
    new BlobServiceClientBuilder()
      .endpoint(endpoint)
      .credential(credential)
      .buildClient()
  }

  private def extractStorageAccountName(profile: com.cardinal.model.StorageProfile): String = {
    // Try to extract from spProperties first, then fall back to role
    profile.spProperties.get("storageAccount") match {
      case Some(account: String) => account
      case _ => 
        // Fall back to using role as storage account name
        profile.role match {
          case role if role != null && role.nonEmpty => role
          case _ => throw new RuntimeException(s"Could not determine storage account name for profile ${profile.storageProfileId}")
        }
    }
  }
}