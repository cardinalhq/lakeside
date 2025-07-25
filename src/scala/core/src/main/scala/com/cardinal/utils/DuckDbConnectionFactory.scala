package com.cardinal.utils

import com.amazonaws.auth.BasicSessionCredentials
import com.cardinal.auth.AwsCredentialsCache
import com.cardinal.auth.AwsCredentialsCache.CREDENTIALS_VALIDITY_SECONDS
import com.cardinal.config.StorageProfileCache
import com.cardinal.core.Environment
import com.cardinal.utils.Commons.STORAGE_PROFILE_CLOUD_PROVIDER_GOOGLE
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

  installHttpfs()

  private def installHttpfs(): Unit = {
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
          "INSTALL '/app/libs/httpfs.duckdb_extension';"
        )

        // execute each, auto-closing the Statement
        cmds.foreach { sql =>
          use(conn.createStatement())
            .executeUpdate(sql)
        }
      }
    }.recover { case e =>
      logger.error("Error installing httpfs", e)
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
        statement.executeUpdate("INSTALL '/app/libs/httpfs.duckdb_extension'")
        statement.executeUpdate("LOAD httpfs")
        statement.executeUpdate(s"SET memory_limit='${DUCKDB_MEMORY_LIMIT}'")
        statement.executeUpdate("SET temp_directory = '/db/duckdb_swap'")
        statement.executeUpdate("SET max_temp_directory_size = '5GB'")
        withS3Credentials(statement, bucketNames)
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
      logger.debug(
        s"Building duckdb secret for storage profile: ${profile.storageProfileId} for" +
        s" cloud provider: ${profile.cloudProvider} bucket: ${profile.bucket}" +
        s" region: ${profile.region} role: ${profile.role}"
      )
      val secretSuffix = profile.storageProfileId.replace("-", "_")

      val isGcp = profile.cloudProvider == STORAGE_PROFILE_CLOUD_PROVIDER_GOOGLE

      val sql = if (isGcp) {
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
      } else {
        val creds    = credentialsCache.getCredentials(profile.role, profile.organizationId)
        var endpoint = profile.endpoint.filter(_.nonEmpty)
          .getOrElse(s"s3.${profile.region}.amazonaws.com")
        val useSsl = profile.useSsl

        if (!useSsl && endpoint.startsWith("http://")) {
          endpoint = endpoint.stripPrefix("http://")
        } else if (useSsl && endpoint.startsWith("https://")) {
          endpoint = endpoint.stripPrefix("https://")
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
      statement.execute(sql)
    }
    statement
  }
}
