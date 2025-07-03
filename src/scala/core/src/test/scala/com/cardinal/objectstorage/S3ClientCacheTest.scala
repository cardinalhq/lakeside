package com.cardinal.objectstorage

import com.cardinal.TestConfig
import com.cardinal.auth.AwsCredentialsCache
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.{ActiveProfiles, ContextConfiguration}

@SpringBootTest
@ContextConfiguration(classes = Array(classOf[TestConfig]))
@ActiveProfiles(Array("test"))
class S3ClientCacheTest {

  @MockBean
  var awsCredentialsCache: AwsCredentialsCache = _



  @Autowired
  var s3ClientCache: S3ClientCache = _

  @BeforeEach
  def setUp(): Unit = {
    // Setup mocks before each test if needed
  }

//  @Test
//  def testGetClientReturnsCachedClientIfPresent(): Unit = {
//    val bucket = "thewardsbackup"
//    val mockClient = mock(classOf[AmazonS3])
//
//
//    when(awsCredentialsCache.getCredentials(any[String], any[String])).thenReturn(mock(classOf[BasicSessionCredentials]))
//   // when(mockClient.getBucketLocation(bucket)).thenReturn("us-west-2")
//
//    val client1 = s3ClientCache.getClient(bucket)
//    val client2 = s3ClientCache.getClient(bucket)
//
//    assertSame(client1, client2)
//
//
//  }

//  @Test
//  def testGetClientCreatesNewClientWhenNotCached(): Unit = {
//    val bucket = "thewardsbackup"
//    val mockProfile = mock(classOf[StorageProfile])
//    when(mockProfile.role).thenReturn("arn:aws:iam::123456789012:role/testRole")
//    when(mockProfile.region).thenReturn("us-west-2")
//
//    when(awsCredentialsCache.getCredentials(any[String], any[String])).thenReturn(mock(classOf[BasicSessionCredentials]))
//
//    val client = s3ClientCache.getClient(bucket)
//
//    assertNotNull(client)
//
//
//  }

//  @Test
//  def testGetClientThrowsExceptionWhenNoStorageProfile(): Unit = {
//    val bucket = "non-existent-bucket"
//
//
//    val exception = assertThrows(classOf[RuntimeException], () => {
//      s3ClientCache.getClient(bucket)
//    })
//
//    assertEquals(s"Could not create client because no storage profile found for bucket! $bucket", exception.getMessage)
//
//  }
}