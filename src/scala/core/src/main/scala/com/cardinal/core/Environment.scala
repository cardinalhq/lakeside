package com.cardinal.core

import com.cardinal.core.Environment.CloudRegion.{CloudRegion, LOCAL}
import org.slf4j.LoggerFactory

import java.net.{HttpURLConnection, URL}
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

object Environment {

  private final val CLOUD_PROVIDER_AWS = "aws"
  private final val CLOUD_PROVIDER_GCP = "gcp"
  private final val CLOUD_PROVIDER_LOCAL = "local"
  private final val metadataBaseUrl = "http://169.254.169.254/latest"

  private val currentRegion: CloudRegion = determineCloudRegion()
  private val debug: Boolean = sys.props.getOrElse("debug", "true").toBoolean

  object CloudRegion extends Enumeration {
    type CloudRegion = Value

    protected case class CloudRegionVal(provider: String, region: String) extends super.Val {
      private def getProvider: String = provider
      def isAws: Boolean = getProvider == CLOUD_PROVIDER_AWS
      def isGcp: Boolean = getProvider == CLOUD_PROVIDER_GCP
      def isLocal: Boolean = getProvider == CLOUD_PROVIDER_LOCAL
    }
    implicit def valueToCloudRegionVal(x: Value): CloudRegionVal = x.asInstanceOf[CloudRegionVal]

    // Local
    val LOCAL: CloudRegionVal = CloudRegionVal("local", "local")

    // unknown
    private val UNKNOWN: CloudRegionVal = CloudRegionVal("unknown", "unknown")

    // Method to parse region string and return appropriate CloudRegionVal using reflection
    def fromRegionString(region: String): CloudRegionVal = {
      if (debug) println(s"Looking up region ->$region<-")
      CloudRegion.values
        .collectFirst {
          case value: CloudRegionVal if value.region == region => value
        }
        .getOrElse(UNKNOWN)
    }

  }

  private def determineCloudRegion(): CloudRegion = {
    if (!isInKubernetes) {
      if (debug) println("Kubernetes is not detected, returning LOCAL")
      LOCAL
    } else {
      if (debug) println("Kubernetes is detected")
      getAWSRegion().orElse(getGCPRegion()).getOrElse(LOCAL)
    }
  }

  private def isInKubernetes: Boolean = sys.env.getOrElse("KUBERNETES_SERVICE_HOST", "").nonEmpty
  def getCurrentRegion: CloudRegion = currentRegion

  private def fetchUrlWithTimeout(url: String, headers: Map[String, String], connectTimeout: Int = 1000, readTimeout: Int = 1000): Option[String] = {
    Try {
      val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(connectTimeout)
      connection.setReadTimeout(readTimeout)
      headers.foreach { case (key, value) =>
        connection.setRequestProperty(key, value)
      }
      Using(Source.fromInputStream(connection.getInputStream))(_.mkString).toOption
    }.toOption.flatten
  }

  private def fetchToken(): Option[String] = {
    val tokenUrl = s"$metadataBaseUrl/api/token"
    if (debug) println(s"[DEBUG] Fetching token from URL: $tokenUrl")

    Try {
      val connection = new URL(tokenUrl).openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("PUT")
      connection.setRequestProperty("X-aws-ec2-metadata-token-ttl-seconds", "21600")
      connection.setConnectTimeout(2000)
      connection.setReadTimeout(2000)
      connection.connect()

      val responseCode = connection.getResponseCode
      if (debug) println(s"[DEBUG] Response code received: $responseCode")

      if (responseCode == 200) {
        val token = Source.fromInputStream(connection.getInputStream).mkString
        if (debug) println(s"[DEBUG] Token fetched successfully")
        connection.disconnect()
        Some(token)
      } else {
        if (debug) println(s"[DEBUG] Failed to fetch token, response code: $responseCode")
        connection.disconnect()
        None
      }
    } match {
      case Success(result) =>
        if (debug) println(s"[DEBUG] Token fetch result: $result")
        result
      case Failure(exception) =>
        if (debug) println(s"[ERROR] Exception occurred while fetching token: ${exception.getMessage}")
        None
    }
  }

  private def fetchUrlWithToken(url: String, token: String): Option[String] = {
    if (debug) println(s"[DEBUG] Starting identity request for URL: $url with token: $token")

    Try {
      val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setRequestProperty("X-aws-ec2-metadata-token", token)
      connection.setConnectTimeout(2000)
      connection.setReadTimeout(2000)
      if (debug) println(s"[DEBUG] Sending request to URL: $url with token: $token")
      connection.connect()

      val responseCode = connection.getResponseCode
      if (debug) println(s"[DEBUG] Response code received: $responseCode")

      if (responseCode == 200) {
        val content = Source.fromInputStream(connection.getInputStream).mkString
        if (debug) println(s"[DEBUG] Successfully fetched content: $content")
        connection.disconnect()
        Some(content)
      } else {
        if (debug) println(s"[DEBUG] Received non-200 response code: $responseCode")
        connection.disconnect()
        None
      }
    } match {
      case Success(result) =>
        if (debug) println(s"[DEBUG] Identity request completed with result: $result")
        result
      case Failure(exception) =>
        if (debug) println(s"[ERROR] Exception occurred while fetching URL: ${exception.getMessage}")
        None
    }
  }

  private def getAWSRegion(): Option[CloudRegion] = {
    val metadataUrl = s"$metadataBaseUrl/dynamic/instance-identity/document"
    val regionRegex = """"region"\s*:\s*"([^"]+)"""".r
    if (debug) println(s"Preparing to fetch metadata with url $metadataUrl")
    for {
      token               <- fetchToken()
      awsIdentityDocument <- fetchUrlWithToken(metadataUrl, token)
      regionMatch         <- regionRegex.findFirstMatchIn(awsIdentityDocument)
    } yield CloudRegion.fromRegionString(regionMatch.group(1))
  }

  private def getGCPRegion(): Option[CloudRegion] = {
    val metadataUrl = "http://metadata.google.internal/computeMetadata/v1/instance/zone"
    val metadataHeader = "Metadata-Flavor" -> "Google"

    fetchUrlWithTimeout(metadataUrl, Map(metadataHeader)).flatMap { zone =>
      val zoneParts = zone.split("/")
      if (zoneParts.nonEmpty) {
        val region = zoneParts.last.split("-").init.mkString("-")
        Some(CloudRegion.fromRegionString(region))
      } else {
        None
      }
    }
  }

  def getSpringProfiles(): List[String] = {
    //TODO: determine dev or test as well

    if (!isInKubernetes) {
      List(CLOUD_PROVIDER_LOCAL)
    } else {
      if (currentRegion.isAws){
        List("aws")
      }else if (currentRegion.isGcp){
        List("gcp")
      }
      else{
        List.empty[String]
      }

    }
  }

}

class Environment{
  def getCurrentRegion: CloudRegion = Environment.currentRegion
}
