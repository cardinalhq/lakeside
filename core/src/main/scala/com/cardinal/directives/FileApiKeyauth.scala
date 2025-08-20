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

import akka.http.scaladsl.model.{HttpHeader, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import com.cardinal.directives.ApiKeyAuth
import com.cardinal.utils.Commons.API_KEY_HEADERS
import org.springframework.stereotype.Component
import org.yaml.snakeyaml.Yaml

import scala.jdk.CollectionConverters._

@Component
class FileApiKeyAuth extends ApiKeyAuth {
  override def checkApiKey: Directive1[String] = {
    optionalHeaderValuePF {
      case h: HttpHeader if (API_KEY_HEADERS.exists(_.equalsIgnoreCase(h.name()))) => h.value()
    }.flatMap {
      case Some(apiKey) =>
        ApiKeyFileCache.findCustomerId(apiKey) match {
          case Some(customerId) => provide(customerId)
          case None             => complete(HttpResponse(StatusCodes.Unauthorized, entity = "Invalid API key"))
        }

      case None =>
        complete(HttpResponse(StatusCodes.Unauthorized, entity = "Missing or invalid API key"))
    }
  }
}

/**
 * Represents an entry mapping an organization to a list of API keys.
 */
case class ApiKeyFileEntry(organizationId: String, keys: List[String])

/**
 * Loads API-key mappings from a YAML file specified by the APIKEY_FILE environment variable.
 * The file is parsed once at startup; subsequent lookups are in-memory.
 */
object ApiKeyFileCache {
  private val fileRef: String = sys.env.getOrElse(
    "API_KEYS_FILE",
    throw new IllegalStateException("Environment variable API_KEYS_FILE must be set")
  )

  private val rawYaml: String = {
    if (fileRef.startsWith("env:")) {
      val envVar = fileRef.substring("env:".length)
      sys.env.getOrElse(
        envVar,
        throw new IllegalStateException(s"Environment variable '$envVar' must be set")
      )
    } else {
      val src = scala.io.Source.fromFile(fileRef)
      try src.mkString
      finally src.close()
    }
  }

  private val entries: List[ApiKeyFileEntry] = {
    val yaml = new Yaml()
    val loaded = yaml.load(rawYaml)
      .asInstanceOf[java.util.List[java.util.Map[String, Any]]]
    loaded.asScala.toList.map { m =>
      val orgId = m.get("organization_id").toString
      val keys  = m.get("keys")
        .asInstanceOf[java.util.List[String]]
        .asScala.toList
      ApiKeyFileEntry(orgId, keys)
    }
  }

  // Look up the organization ID for the given API key, returning the first match if any.
  def findCustomerId(apiKey: String): Option[String] =
    entries.collectFirst {
      case ApiKeyFileEntry(orgId, ks) if ks.contains(apiKey) => orgId
    }
}
