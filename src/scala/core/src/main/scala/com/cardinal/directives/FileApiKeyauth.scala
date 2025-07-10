package com.cardinal.auth

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import com.cardinal.directives.ApiKeyAuth
import com.cardinal.utils.Commons.API_KEY_HEADER
import org.springframework.stereotype.Component
import org.yaml.snakeyaml.Yaml

import java.io.FileInputStream
import scala.jdk.CollectionConverters._

@Component
class FileApiKeyAuth extends ApiKeyAuth {
  override def checkApiKey: Directive1[String] =
    optionalHeaderValueByName(API_KEY_HEADER).flatMap {
      case Some(apiKey) =>
        ApiKeyFileCache.findCustomerId(apiKey) match {
          case Some(customerId) => provide(customerId)
          case None             => complete(HttpResponse(StatusCodes.Unauthorized, entity = "Invalid API key"))
        }

      case None =>
        complete(HttpResponse(StatusCodes.Unauthorized, entity = "Missing or invalid API key"))
    }
}

/**
 * Represents an entry mapping an organization to a list of API keys.
 */
case class ApiKeyFileEntry(
                            organizationId: String,
                            keys: List[String]
                          )

/**
 * Loads API-key mappings from a YAML file specified by the APIKEY_FILE environment variable.
 * The file is parsed once at startup; subsequent lookups are in-memory.
 */
object ApiKeyFileCache {
  // Path to YAML file (must be set)
  private val filePath: String = sys.env.getOrElse(
    "API_KEYS_FILE",
    throw new IllegalStateException("Environment variable API_KEYS_FILE must be set")
  )

  // Load and parse YAML once
  private val entries: List[ApiKeyFileEntry] = {
    val yaml = new Yaml()
    val input = new FileInputStream(filePath)
    try {
      // Expecting a list of maps
      val loaded = yaml.load(input)
        .asInstanceOf[java.util.List[java.util.Map[String, Any]]]
      loaded.asScala.toList.map { m =>
        val orgId = m.get("organization_id").toString
        val keys = m.get("keys")
          .asInstanceOf[java.util.List[String]]
          .asScala.toList
        ApiKeyFileEntry(orgId, keys)
      }
    } finally {
      input.close()
    }
  }

  /**
   * Look up the organization ID for the given API key, returning the first match if any.
   */
  def findCustomerId(apiKey: String): Option[String] =
    entries.collectFirst {
      case ApiKeyFileEntry(orgId, ks) if ks.contains(apiKey) => orgId
    }
}
