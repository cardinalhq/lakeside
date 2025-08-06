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

package com.cardinal


import com.amazonaws.services.securitytoken.AWSSecurityTokenService
import com.cardinal.auth.AwsCredentialsCache
import com.netflix.atlas.akka.AkkaConfiguration
import com.netflix.iep.spring.IepConfiguration
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.cache.annotation.EnableCaching
import org.springframework.context.annotation._

@EnableCaching
@Configuration
@Import(Array(classOf[IepConfiguration], classOf[AkkaConfiguration], classOf[CoreConfiguration]))
@ComponentScan(basePackages = Array("com.cardinal"))
class TestConfig {

  @MockBean
  var mockStsClient: AWSSecurityTokenService = _

  @Bean
  def awsCredentialsCache(stsClient: AWSSecurityTokenService): AwsCredentialsCache = new AwsCredentialsCache(stsClient)


}
