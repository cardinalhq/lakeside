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
