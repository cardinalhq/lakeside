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

import com.amazonaws.auth.{AWSCredentials, BasicSessionCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.securitytoken.AWSSecurityTokenService
import com.amazonaws.services.securitytoken.model.{AssumeRoleRequest, AssumeRoleResult}
import org.slf4j.LoggerFactory
import org.springframework.cache.annotation.{CacheConfig, Cacheable}

@CacheConfig(cacheNames = Array("credentials"), cacheManager = "credentialCacheManager")
class AwsCredentialsCache(private val stsClient: AWSSecurityTokenService) {
  private val logger = LoggerFactory.getLogger(getClass)
  // default provider to cover EC2/ECS/ENV/IAM-role etc.
  private val defaultProvider = DefaultAWSCredentialsProviderChain.getInstance()

  /**
   * If roleArn is null or blank, return whatever the default chain gives you.
   * Otherwise, assume the given role and cache that session.
   */
  @Cacheable(
    value = Array("credentials"),
    key   = "#roleArn != null && #roleArn.trim() != '' ? #roleArn : 'default'",
    sync  = true
  )
  def getCredentials(roleArn: String, externalId: String): AWSCredentials = {
    if (roleArn == null || roleArn.trim.isEmpty) {
      logger.debug("No roleArn provided – falling back to DefaultAWSCredentialsProviderChain.")
      defaultProvider.getCredentials
    } else {
      logger.debug(s"Assuming role $roleArn with externalId $externalId.")
      assumeRole(roleArn, externalId)
    }
  }

  private def assumeRole(roleArn: String, externalId: String): BasicSessionCredentials = {
    try {
      val req = new AssumeRoleRequest()
        .withRoleArn(roleArn)
        .withExternalId(externalId)
        .withDurationSeconds(AwsCredentialsCache.CREDENTIALS_VALIDITY_SECONDS)
        .withRoleSessionName("CardinalHQ-" + System.currentTimeMillis())

      val res: AssumeRoleResult = stsClient.assumeRole(req)

      new BasicSessionCredentials(
        res.getCredentials.getAccessKeyId,
        res.getCredentials.getSecretAccessKey,
        res.getCredentials.getSessionToken
      )
    } catch {
      case e: Exception =>
        logger.error(s"Error assuming role $roleArn: ${e.getMessage}", e)
        throw new RuntimeException(s"Error assuming role $roleArn", e)
    }
  }
}

object AwsCredentialsCache {
  val CREDENTIALS_VALIDITY_SECONDS: Int = 60 * 60
}
