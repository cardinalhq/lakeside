package com.cardinal.auth

import akka.http.scaladsl.model.headers.RawHeader
import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.{DecodedJWT, JWTVerifier}
import com.cardinal.utils.Commons.{AUTH_TOKEN_HEADER, AUTH_TOKEN_ORG_ID_CLAIM}

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import scala.util.Try

object AuthToken {

  private val issuer = "cardinalhq.io"
  private val algorithm = Algorithm.HMAC256("D35623990536211F8EA473B42ED115A124906F538C00B86BF3DA13810E55AD59")
  private val jwtVerifier: JWTVerifier =
    JWT
      .require(algorithm)
      .withIssuer(issuer)
      .build()

  private val orgIdTokens = new ConcurrentHashMap[String, String]()

  def validate(tokenString: String): DecodedJWT = {
    try {
      jwtVerifier.verify(tokenString)
    }
    catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
  }

  def getAuthHeader(orgId: String): Seq[RawHeader] = {
    var token = orgIdTokens.computeIfAbsent(orgId, (_: String) => {
      issue(orgId)
    })
    token = Try(validate(token).getToken).getOrElse(issue(orgId))
    orgIdTokens.put(orgId, token)

    Seq[RawHeader](RawHeader(name = "Cookie", value = s"$AUTH_TOKEN_HEADER=$token"))
  }

  private def issue(orgId: String) = {
    JWT
      .create()
      .withIssuer(issuer)
      .withClaim(AUTH_TOKEN_ORG_ID_CLAIM, orgId)
      .withExpiresAt(Instant.now().plus(1, ChronoUnit.HOURS))
      .sign(algorithm)
  }
}
