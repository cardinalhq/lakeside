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

package com.cardinal.utils

import com.amazonaws.auth.BasicSessionCredentials
import com.cardinal.auth.AwsCredentialsCache
import com.cardinal.auth.AwsCredentialsCache.CREDENTIALS_VALIDITY_SECONDS
import com.cardinal.config.StorageProfileCache
import com.cardinal.core.Environment
import com.cardinal.utils.Commons.{STORAGE_PROFILE_CLOUD_PROVIDER_GOOGLE, STORAGE_PROFILE_CLOUD_PROVIDER_AZURE}
import org.duckdb.DuckDBConnection
import org.slf4j.LoggerFactory

import java.sql.{DriverManager, Statement}
import java.time.Instant
import scala.collection.mutable
import scala.util.{Try, Using}

case class ExpiringConnection(connection: DuckDBConnection, expirationTime: Instant)

object DuckDbConnectionFactory {
  private val logger = LoggerFactory.getLogger(getClass)
  private[utils] lazy val credentialsCache: AwsCredentialsCache = SpringContextUtil.getBean(classOf[AwsCredentialsCache])

  private[utils] var storageProfileCache =  SpringContextUtil.getBean(classOf[StorageProfileCache])

  private[utils] val sealedReadConnections: ThreadLocal[mutable.Map[Set[String], ExpiringConnection]] =
    ThreadLocal.withInitial(() => mutable.Map.empty)

  private val DUCKDB_MEMORY_LIMIT = sys.env.getOrElse("DUCKDB_MEMORY_LIMIT", "4GB")

  installExtensions()

  private def installExtensions(): Unit = {
    Try {
      Using.Manager { use =>
        // open the connection once
        val conn = use(
          DriverManager
            .getConnection("jdbc:duckdb:/db/shared.duckdb")
            .asInstanceOf[DuckDBConnection]
        )

        // list of commands to run
        val cmds = Seq(
          "INSTALL '/app/libs/httpfs.duckdb_extension';",
          "INSTALL '/app/libs/azure.duckdb_extension';"
        )

        // execute each, auto-closing the Statement
        cmds.foreach { sql =>
          use(conn.createStatement())
            .executeUpdate(sql)
        }
      }
    }.recover { case e =>
      logger.error("Error installing extensions", e)
    }
  }

  def getSealedReadConnection(bucketNames: Set[String]): DuckDBConnection = {
    val now = Instant.now()
    val connectionOpt = sealedReadConnections.get().get(bucketNames)
    connectionOpt match {
      case Some(expiringConnection) if expiringConnection.expirationTime.isAfter(now) =>
        logger.debug("Reusing existing sealed read connection.")
        val conn = expiringConnection.connection
        val statement = conn.createStatement()
        withS3Credentials(statement, bucketNames)
        statement.close()
        conn
      case _ =>
        connectionOpt.foreach(_.connection.close()) // Close the connection if it is expired, so that we don't leak them
        logger.info("Initializing new sealed read connection with provided bucket names.")
        val connection = createConnection("jdbc:duckdb:")
        val statement = connection.createStatement()
        
        logger.debug("Installing and loading DuckDB extensions...")
        statement.executeUpdate("INSTALL '/app/libs/httpfs.duckdb_extension'")
        logger.debug("✓ Installed httpfs extension")
        statement.executeUpdate("LOAD httpfs")
        logger.debug("✓ Loaded httpfs extension")
        
        statement.executeUpdate("INSTALL '/app/libs/azure.duckdb_extension'")
        logger.debug("✓ Installed azure extension")
        statement.executeUpdate("LOAD azure")
        logger.debug("✓ Loaded azure extension")
        
        statement.executeUpdate("SET azure_transport_option_type = 'curl'")
        logger.info("✓ Set azure_transport_option_type = 'curl'")
        
        statement.executeUpdate(s"SET memory_limit='${DUCKDB_MEMORY_LIMIT}'")
        logger.debug(s"✓ Set memory_limit to ${DUCKDB_MEMORY_LIMIT}")
        
        statement.executeUpdate("SET temp_directory = '/db/duckdb_swap'")
        statement.executeUpdate("SET max_temp_directory_size = '5GB'")
        logger.debug("✓ Set temp directory settings")
        
        logger.info(s"About to configure credentials for buckets: ${bucketNames.mkString(", ")}")
        withS3Credentials(statement, bucketNames)
        logger.info("✓ Completed credential configuration")
        
        statement.close()
        val newExpirationTime = now.plusSeconds(4 * 60) // tokens should renew about 5 minutes before they expire, but this should be done differently
        val connectionWithExpiration = ExpiringConnection(connection, newExpirationTime)
        sealedReadConnections.get().update(bucketNames, connectionWithExpiration)
        connection
    }
  }

  def getLocalParquetConnection: DuckDBConnection = {
    val connection = createConnection(s"jdbc:duckdb:")
    val statement = connection.createStatement()
    statement.close()
    connection
  }

  private def createConnection(connectionPath: String) = {
    val connection = DriverManager
      .getConnection(connectionPath)
      .asInstanceOf[DuckDBConnection]
    connection
  }

  //TODO: temporarily used for GCS, and used as fallback if no role specified
  private val S3_ACCESS_KEY_ID = EnvUtils.firstEnv(Seq("S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"))
  private val SECRET_ACCESS_KEY = EnvUtils.firstEnv(Seq("S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"))

  private def withS3Credentials(statement: Statement, bucketNames: Set[String]): Statement = {
    logger.debug(s"Building s3 credentials for buckets: $bucketNames ")
    val storageProfiles = bucketNames.flatMap { bucketName =>
      storageProfileCache.getStorageProfile(bucketName) match {
        case Some(profile) =>
          logger.trace(s"Found storage profile for bucket: $bucketName")
          Some(profile)
        case None =>
          logger.error(s"Storage profile not found for bucket: $bucketName")
          None
      }
    }

    storageProfiles.foreach { profile =>
      logger.info(
        s"Building duckdb secret for storage profile: ${profile.storageProfileId} for" +
        s" cloud provider: ${profile.cloudProvider} bucket: ${profile.bucket}" +
        s" region: ${profile.region} role: ${profile.role}"
      )
      val secretSuffix = profile.storageProfileId.replace("-", "_")

      val isGcp = profile.cloudProvider == STORAGE_PROFILE_CLOUD_PROVIDER_GOOGLE
      val isAzure = profile.cloudProvider == STORAGE_PROFILE_CLOUD_PROVIDER_AZURE
      
      logger.info(s"Profile ${profile.storageProfileId}: isGcp=$isGcp, isAzure=$isAzure, cloudProvider='${profile.cloudProvider}'")

      val sql = if (isGcp) {
        logger.info(s"Taking GCP path for ${profile.storageProfileId}")
        s"""
           |CREATE OR REPLACE SECRET secret_$secretSuffix (
           |  TYPE GCS,
           |  ENDPOINT 'storage.googleapis.com',
           |  URL_STYLE 'path',
           |  KEY_ID '$S3_ACCESS_KEY_ID',
           |  SECRET '$SECRET_ACCESS_KEY',
           |  REGION '${profile.region}',
           |  SCOPE 'gcs://${profile.bucket}'
           |);
        """.stripMargin.trim
      } else if (isAzure) {
        logger.info(s"Taking AZURE path for ${profile.storageProfileId}")
        logger.debug(s"Azure profile details: bucket=${profile.bucket}, endpoint=${profile.endpoint}, region=${profile.region}")
        
        val storageAccount = extractStorageAccountFromEndpoint(profile.endpoint)
        logger.info(s"Extracted storage account: '$storageAccount' from endpoint: ${profile.endpoint}")
        
        // Check for Azure service principal credentials in environment
        val azureClientId = sys.env.get("AZURE_CLIENT_ID")
        val azureClientSecret = sys.env.get("AZURE_CLIENT_SECRET") 
        val azureTenantId = sys.env.get("AZURE_TENANT_ID")
        
        logger.info(s"Azure environment variables check:")
        logger.info(s"  AZURE_CLIENT_ID: ${azureClientId.map(id => s"${id.take(8)}...${id.takeRight(4)}").getOrElse("NOT SET")}")
        logger.info(s"  AZURE_CLIENT_SECRET: ${azureClientSecret.map(_ => "SET (hidden)").getOrElse("NOT SET")}")
        logger.info(s"  AZURE_TENANT_ID: ${azureTenantId.map(id => s"${id.take(8)}...${id.takeRight(4)}").getOrElse("NOT SET")}")
        
        val sql = (azureClientId, azureClientSecret, azureTenantId) match {
          case (Some(clientId), Some(clientSecret), Some(tenantId)) =>
            // Use explicit service principal credentials when available
            logger.info(
              s"✓ All Azure service principal credentials found - using service_principal provider " +
                s"for ${profile.storageProfileId} bucket=${profile.bucket}, storageAccount=$storageAccount"
            )
            val generatedSql = s"""
               |CREATE OR REPLACE SECRET secret_$secretSuffix (
               |  TYPE azure,
               |  PROVIDER service_principal,
               |  TENANT_ID '$tenantId',
               |  CLIENT_ID '$clientId',
               |  CLIENT_SECRET '$clientSecret',
               |  ACCOUNT_NAME '$storageAccount',
               |  SCOPE 'az://${profile.bucket}/'
               |);
            """.stripMargin.trim
            logger.info(s"Generated service_principal SQL for secret_$secretSuffix:")
            logger.info(s"$generatedSql")
            generatedSql
          case _ =>
            // Fallback to credential_chain if environment variables not available
            logger.warn(
              s"⚠ Missing Azure service principal credentials - falling back to credential_chain " +
                s"for ${profile.storageProfileId} bucket=${profile.bucket}, storageAccount=$storageAccount"
            )
            val generatedSql = s"""
               |CREATE OR REPLACE SECRET secret_$secretSuffix (
               |  TYPE azure,
               |  PROVIDER credential_chain,
               |  ACCOUNT_NAME '$storageAccount',
               |  SCOPE 'az://${profile.bucket}/'
               |);
            """.stripMargin.trim
            logger.info(s"Generated credential_chain SQL for secret_$secretSuffix:")
            logger.info(s"$generatedSql")
            generatedSql
        }
        
        logger.info(s"About to execute Azure secret creation SQL for profile ${profile.storageProfileId}")
        sql
      } else {
        logger.info(s"Taking AWS/S3 path for ${profile.storageProfileId}")
        val creds    = credentialsCache.getCredentials(profile.role, profile.organizationId)
        var endpoint = profile.endpoint.filter(_.nonEmpty)
          .getOrElse(s"s3.${profile.region}.amazonaws.com")
        var useSsl = true

        if(endpoint.nonEmpty) {
          if (endpoint.startsWith("http://")) {
            endpoint = endpoint.stripPrefix("http://")
            useSsl = false
          } else if (endpoint.startsWith("https://")) {
            endpoint = endpoint.stripPrefix("https://")
            useSsl = true
          }
        }

        logger.debug(
          s"Creating S3 secret for ${profile.storageProfileId} " +
            s"bucket=${profile.bucket}, region=${profile.region}, role=${profile.role}, " +
            s"endpoint=$endpoint"
        )

        val baseLines = Seq(
          s"TYPE S3",
          s"ENDPOINT '$endpoint'",
          s"URL_STYLE 'path'",
          s"USE_SSL '$useSsl'",
          s"KEY_ID '${creds.getAWSAccessKeyId}'",
          s"SECRET '${creds.getAWSSecretKey}'"
        )

        val sessionLine = creds match {
          case b: BasicSessionCredentials =>
            Seq(s"SESSION_TOKEN '${b.getSessionToken}'")
          case _ =>
            Seq.empty
        }

        val tailLines = Seq(
          s"REGION '${profile.region}'",
          s"SCOPE 's3://${profile.bucket}'"
        )

        val allLines = baseLines ++ sessionLine ++ tailLines
        s"""
           |CREATE OR REPLACE SECRET secret_$secretSuffix (
           |  ${allLines.mkString(",\n  ")}
           |);
        """.stripMargin.trim
      }

      // now execute whichever you built
      logger.info(s"Executing SQL statement for profile ${profile.storageProfileId}...")
      try {
        val result = statement.execute(sql)
        logger.info(s"✓ Successfully executed secret creation SQL for profile ${profile.storageProfileId}, result: $result")
      } catch {
        case e: Exception =>
          logger.error(s"✗ Failed to execute secret creation SQL for profile ${profile.storageProfileId}: ${e.getMessage}")
          logger.error(s"Failed SQL was: $sql")
          throw e
      }
    }
    statement
  }

  private def extractStorageAccountFromEndpoint(endpoint: Option[String]): String = {
    logger.debug(s"Extracting storage account from endpoint: $endpoint")
    
    val result = endpoint match {
      case Some(ep) if ep.contains("blob.core.windows.net") =>
        val account = ep.split("\\.")(0).replace("https://", "").replace("http://", "")
        logger.debug(s"Extracted storage account '$account' using blob.core.windows.net pattern from '$ep'")
        account
      case Some(ep) => 
        // Fallback: try to extract from any endpoint format
        val account = ep.replace("https://", "").replace("http://", "").split("\\.")(0)
        logger.debug(s"Extracted storage account '$account' using fallback pattern from '$ep'")
        account
      case None => 
        logger.error("Azure endpoint is None - cannot extract storage account")
        throw new RuntimeException("Azure endpoint required for storage account extraction")
    }
    
    logger.info(s"Final extracted storage account: '$result'")
    result
  }
}
