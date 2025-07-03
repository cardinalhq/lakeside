package com.cardinal.directives

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Directive1
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component

/**
 * Enabled when `auth.apikey.enabled=false` (or missing).
 * Always fails, so users can’t fall back to API-key.
 */
@Component
@ConditionalOnProperty(name = Array("auth.apikey.enabled"), havingValue = "false", matchIfMissing = true)
class NoOpApiKeyAuth extends ApiKeyAuth {
  override def checkApiKey: Directive1[String] =
    complete(HttpResponse(StatusCodes.Unauthorized, "Missing auth token"))
}
