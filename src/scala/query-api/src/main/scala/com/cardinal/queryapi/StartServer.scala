package com.cardinal.queryapi

import akka.http.scaladsl.server.Directives
import com.cardinal.CoreConfiguration
import com.cardinal.core.CardinalSpringApplication
import com.netflix.atlas.akka.AkkaConfiguration
import com.netflix.iep.spring.IepConfiguration
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cache.annotation.EnableCaching

@EnableCaching
@SpringBootApplication(
  scanBasePackageClasses =
    Array[Class[_]](classOf[CoreConfiguration],classOf[IepConfiguration], classOf[AkkaConfiguration], classOf[QueryApiConfiguration])
)
class StartServer extends CommandLineRunner with Directives {
  override def run(args: String*): Unit = {}
}

object StartServer {
  def main(args: Array[String]): Unit = {
    val app = new CardinalSpringApplication(classOf[StartServer])
    app.run(args: _*)
  }
}
