package com.cardinal.directives

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Directive1
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component

/**
 * Enabled when `auth.apikey.enabled=true` in application.yml.
 */
@Component
@ConditionalOnProperty(name = Array("auth.apikey.enabled"), havingValue = "true")
class DefaultApiKeyAuth extends ApiKeyAuth {
  override def checkApiKey: Directive1[String] =
    optionalHeaderValueByName(API_KEY_HEADER).flatMap {
      case Some(apiKey) =>
        ApiKeyCache.getCustomerId(apiKey) match {
          case Some(customerId) =>
            DEMO_ORG_ID match {
              case Some(demoOrgId) if customerId == demoOrgId =>
                complete(HttpResponse(StatusCodes.Unauthorized, "Unauthorized access for demo organization"))
              case _ =>
                provide(customerId)
            }
          case None =>
            complete(HttpResponse(StatusCodes.Unauthorized, "Invalid apiKey"))
        }

      case None =>
        complete(HttpResponse(StatusCodes.Unauthorized, "Missing or invalid API key"))
    }
}
