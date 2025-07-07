package com.cardinal.directives

import akka.http.scaladsl.server.Directive1

trait ApiKeyAuth {
  def checkApiKey: Directive1[String]
}
