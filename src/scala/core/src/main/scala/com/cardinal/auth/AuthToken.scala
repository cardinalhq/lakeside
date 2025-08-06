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

  // Load the HMAC256 key from the environment (throws if missing)
  val secretKey: String =
    sys.env.getOrElse(
      "TOKEN_HMAC256_KEY",
      throw new IllegalStateException("Environment variable TOKEN_HMAC256_KEY must be set")
    )

  private val algorithm: Algorithm = Algorithm.HMAC256("" + secretKey)

  private val jwtVerifier: JWTVerifier =
    JWT
      .require(algorithm)
      .withIssuer(issuer)
      .build()

  private val orgIdTokens = new ConcurrentHashMap[String, String]()

  def validate(tokenString: String): DecodedJWT =
    try {
      jwtVerifier.verify(tokenString)
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }

  def getAuthHeader(orgId: String): Seq[RawHeader] = {
    var token = orgIdTokens.computeIfAbsent(orgId, (_: String) => issue(orgId))
    token = Try(validate(token).getToken).getOrElse(issue(orgId))
    orgIdTokens.put(orgId, token)

    Seq(RawHeader(name = "Cookie", value = s"$AUTH_TOKEN_HEADER=$token"))
  }

  private def issue(orgId: String): String =
    JWT
      .create()
      .withIssuer(issuer)
      .withClaim(AUTH_TOKEN_ORG_ID_CLAIM, orgId)
      .withExpiresAt(Instant.now().plus(1, ChronoUnit.HOURS))
      .sign(algorithm)
}
