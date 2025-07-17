package com.cardinal.directives

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.{Directive1, Directives}
import com.cardinal.auth.AuthToken
import com.cardinal.utils.Commons._
import com.netflix.atlas.akka.WebApi
import org.springframework.stereotype.Component

import scala.concurrent.{ExecutionContext, Future}

@Component
abstract class AuthDirectives(actorSystem: ActorSystem, apiKeyAuth: ApiKeyAuth) extends WebApi with Directives {
  require(apiKeyAuth != null, "apiKeyAuth must not be null")

  implicit val as: ActorSystem      = actorSystem
  implicit val ec: ExecutionContext = actorSystem.dispatcher

  def auth: Directive1[String] =
    optionalCookie(AUTH_TOKEN_HEADER).flatMap {
      case Some(cookie) =>
        val tokenString = cookie.value
        onSuccess(Future { AuthToken.validate(tokenString) }).flatMap {
          case null =>
            complete(HttpResponse(StatusCodes.Unauthorized, entity = "Invalid auth token"))

          case decodedToken =>
            Option(decodedToken.getClaim(AUTH_TOKEN_ORG_ID_CLAIM).asString()) match {
              case Some(orgId) =>
                provide(orgId)

              case None =>
                complete(HttpResponse(
                  StatusCodes.Unauthorized,
                  entity = "JWT does not contain required 'org_id' claim"
                ))
            }
        }

      case None =>
        apiKeyAuth.checkApiKey
    }
}
