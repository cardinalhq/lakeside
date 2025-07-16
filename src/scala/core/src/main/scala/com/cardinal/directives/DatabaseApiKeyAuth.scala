package com.cardinal.directives

import akka.http.scaladsl.model.{HttpHeader, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Directive1
import com.cardinal.utils.Commons.API_KEY_HEADERS
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression
import org.springframework.stereotype.Component

@Component
@ConditionalOnExpression("#{systemEnvironment['API_KEY_FILE'] == null}")
class DatabaseApiKeyAuth extends ApiKeyAuth {
  override def checkApiKey: Directive1[String] = {
    optionalHeaderValuePF {
      case h: HttpHeader if API_KEY_HEADERS.exists(_.equalsIgnoreCase(h.name())) => h.value()
    }.flatMap {
      case Some(apiKey) =>
        ApiKeyCache.getCustomerId(apiKey) match {
          case Some(customerId) => provide(customerId)
          case None             => complete(HttpResponse(StatusCodes.Unauthorized, entity = "Invalid API key"))
        }

      case None =>
        complete(HttpResponse(StatusCodes.Unauthorized, entity = "Missing or invalid API key"))
    }
  }
}
