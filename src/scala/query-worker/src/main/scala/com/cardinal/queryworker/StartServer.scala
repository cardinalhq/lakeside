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

package com.cardinal.queryworker

import akka.http.scaladsl.server.Directives
import com.cardinal.CoreConfiguration
import com.cardinal.core.CardinalSpringApplication
import com.netflix.atlas.akka.AkkaConfiguration
import com.netflix.iep.spring.IepConfiguration
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cache.annotation.EnableCaching

import javax.inject.Inject

@EnableCaching
@SpringBootApplication(
  scanBasePackageClasses = Array[Class[_]](
    classOf[CoreConfiguration],
    classOf[QueryWorkerConfiguration],
    classOf[IepConfiguration],
    classOf[AkkaConfiguration]
  )
)
class StartServer @Inject() extends CommandLineRunner with Directives {
  override def run(args: String*): Unit = {}
}

object StartServer {
  def main(args: Array[String]): Unit = {
    val app = new CardinalSpringApplication(classOf[StartServer])
    app.run(args: _*)
  }
}
