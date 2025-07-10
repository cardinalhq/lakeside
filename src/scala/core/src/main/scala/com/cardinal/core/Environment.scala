package com.cardinal.core

import com.cardinal.core.Environment.CloudRegion.{CloudRegion, LOCAL}

import java.net.{HttpURLConnection, URL}
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

object Environment {

  final val CLOUD_PROVIDER_AWS = "aws"
  final val CLOUD_PROVIDER_GCP = "gcp"
  final val CLOUD_PROVIDER_LOCAL = "local"
  private final val metadataBaseUrl = "http://169.254.169.254/latest"

  private val currentRegion: CloudRegion = determineCloudRegion()
  private val debug: Boolean = sys.props.getOrElse("debug", "true").toBoolean

  object CloudRegion extends Enumeration {
    type CloudRegion = Value

    protected case class CloudRegionVal(provider: String, region: String) extends super.Val {
      def getProvider: String = provider
      def isAws: Boolean = getProvider == CLOUD_PROVIDER_AWS
      def isGcp: Boolean = getProvider == CLOUD_PROVIDER_GCP
      def isLocal: Boolean = getProvider == CLOUD_PROVIDER_LOCAL
    }
    implicit def valueToCloudRegionVal(x: Value): CloudRegionVal = x.asInstanceOf[CloudRegionVal]

    // AWS Regions
    val AWS_US_EAST_1: CloudRegionVal = CloudRegionVal("aws", "us-east-1")
    val AWS_US_EAST_2: CloudRegionVal = CloudRegionVal("aws", "us-east-2")
    val AWS_US_WEST_1: CloudRegionVal = CloudRegionVal("aws", "us-west-1")
    val AWS_US_WEST_2: CloudRegionVal = CloudRegionVal("aws", "us-west-2")
    val AWS_CA_CENTRAL_1: CloudRegionVal = CloudRegionVal("aws", "ca-central-1")
    val AWS_EU_WEST_1: CloudRegionVal = CloudRegionVal("aws", "eu-west-1")
    val AWS_EU_WEST_2: CloudRegionVal = CloudRegionVal("aws", "eu-west-2")
    val AWS_EU_WEST_3: CloudRegionVal = CloudRegionVal("aws", "eu-west-3")
    val AWS_EU_CENTRAL_1: CloudRegionVal = CloudRegionVal("aws", "eu-central-1")
    val AWS_EU_NORTH_1: CloudRegionVal = CloudRegionVal("aws", "eu-north-1")
    val AWS_EU_SOUTH_1: CloudRegionVal = CloudRegionVal("aws", "eu-south-1")
    val AWS_AP_SOUTHEAST_1: CloudRegionVal = CloudRegionVal("aws", "ap-southeast-1")
    val AWS_AP_SOUTHEAST_2: CloudRegionVal = CloudRegionVal("aws", "ap-southeast-2")
    val AWS_AP_SOUTHEAST_3: CloudRegionVal = CloudRegionVal("aws", "ap-southeast-3")
    val AWS_AP_NORTHEAST_1: CloudRegionVal = CloudRegionVal("aws", "ap-northeast-1")
    val AWS_AP_NORTHEAST_2: CloudRegionVal = CloudRegionVal("aws", "ap-northeast-2")
    val AWS_AP_NORTHEAST_3: CloudRegionVal = CloudRegionVal("aws", "ap-northeast-3")
    val AWS_AP_EAST_1: CloudRegionVal = CloudRegionVal("aws", "ap-east-1")
    val AWS_AP_SOUTH_1: CloudRegionVal = CloudRegionVal("aws", "ap-south-1")
    val AWS_SA_EAST_1: CloudRegionVal = CloudRegionVal("aws", "sa-east-1")
    val AWS_AF_SOUTH_1: CloudRegionVal = CloudRegionVal("aws", "af-south-1")
    val AWS_ME_SOUTH_1: CloudRegionVal = CloudRegionVal("aws", "me-south-1")
    val AWS_ME_CENTRAL_1: CloudRegionVal = CloudRegionVal("aws", "me-central-1")

    // GCP Regions
    val GCP_US_CENTRAL1: CloudRegionVal = CloudRegionVal("gcp", "us-central1")
    val GCP_US_EAST1: CloudRegionVal = CloudRegionVal("gcp", "us-east1")
    val GCP_US_EAST4: CloudRegionVal = CloudRegionVal("gcp", "us-east4")
    val GCP_US_WEST1: CloudRegionVal = CloudRegionVal("gcp", "us-west1")
    val GCP_US_WEST2: CloudRegionVal = CloudRegionVal("gcp", "us-west2")
    val GCP_US_WEST3: CloudRegionVal = CloudRegionVal("gcp", "us-west3")
    val GCP_US_WEST4: CloudRegionVal = CloudRegionVal("gcp", "us-west4")
    val GCP_ASIA_EAST1: CloudRegionVal = CloudRegionVal("gcp", "asia-east1")
    val GCP_ASIA_EAST2: CloudRegionVal = CloudRegionVal("gcp", "asia-east2")
    val GCP_ASIA_NORTHEAST1: CloudRegionVal = CloudRegionVal("gcp", "asia-northeast1")
    val GCP_ASIA_NORTHEAST2: CloudRegionVal = CloudRegionVal("gcp", "asia-northeast2")
    val GCP_ASIA_NORTHEAST3: CloudRegionVal = CloudRegionVal("gcp", "asia-northeast3")
    val GCP_ASIA_SOUTH1: CloudRegionVal = CloudRegionVal("gcp", "asia-south1")
    val GCP_ASIA_SOUTH2: CloudRegionVal = CloudRegionVal("gcp", "asia-south2")
    val GCP_ASIA_SOUTHEAST1: CloudRegionVal = CloudRegionVal("gcp", "asia-southeast1")
    val GCP_ASIA_SOUTHEAST2: CloudRegionVal = CloudRegionVal("gcp", "asia-southeast2")
    val GCP_AUSTRALIA_SOUTHEAST1: CloudRegionVal = CloudRegionVal("gcp", "australia-southeast1")
    val GCP_AUSTRALIA_SOUTHEAST2: CloudRegionVal = CloudRegionVal("gcp", "australia-southeast2")
    val GCP_EUROPE_CENTRAL2: CloudRegionVal = CloudRegionVal("gcp", "europe-central2")
    val GCP_EUROPE_NORTH1: CloudRegionVal = CloudRegionVal("gcp", "europe-north1")
    val GCP_EUROPE_WEST1: CloudRegionVal = CloudRegionVal("gcp", "europe-west1")
    val GCP_EUROPE_WEST2: CloudRegionVal = CloudRegionVal("gcp", "europe-west2")
    val GCP_EUROPE_WEST3: CloudRegionVal = CloudRegionVal("gcp", "europe-west3")
    val GCP_EUROPE_WEST4: CloudRegionVal = CloudRegionVal("gcp", "europe-west4")
    val GCP_EUROPE_WEST6: CloudRegionVal = CloudRegionVal("gcp", "europe-west6")
    val GCP_NORTHAMERICA_NORTHEAST1: CloudRegionVal = CloudRegionVal("gcp", "northamerica-northeast1")
    val GCP_NORTHAMERICA_NORTHEAST2: CloudRegionVal = CloudRegionVal("gcp", "northamerica-northeast2")
    val GCP_SOUTHAMERICA_EAST1: CloudRegionVal = CloudRegionVal("gcp", "southamerica-east1")
    val GCP_SOUTHAMERICA_WEST1: CloudRegionVal = CloudRegionVal("gcp", "southamerica-west1")


    // Local
    val LOCAL: CloudRegionVal = CloudRegionVal("local", "local")

    // unknown
    val UNKNOWN: CloudRegionVal = CloudRegionVal("unknown", "unknown")

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

  def isInKubernetes: Boolean = sys.env.getOrElse("KUBERNETES_SERVICE_HOST", "").nonEmpty
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
  def getCurrentRegion = Environment.currentRegion
}
