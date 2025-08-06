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

package com.cardinal.config

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, PrintWriter}

class StorageProfileCacheFileSpec extends AnyFunSuite with Matchers {

  private val testYaml =
    """
      |- organization_id: "org-1"
      |  instance_num: 1
      |  collector_name: "test-collector"
      |  cloud_provider: "ceph"
      |  region: "ceph-objectstore"
      |  bucket: "bucket-1"
      |  role: ""
      |  insecure_tls: true
      |  use_path_style: true
      |
      |- organization_id: "org-2"
      |  instance_num: 2
      |  collector_name: "another-collector"
      |  cloud_provider: "aws"
      |  region: "us-east-2"
      |  bucket: "bucket-2"
      |  role: "arn:aws:iam::123456789012:role/MyRole"
      |""".stripMargin

  test("getStorageProfile by bucket") {
    val cache = StorageProfileCacheFile.fromYamlString(testYaml)

    val found = cache.getStorageProfile("bucket-2")
    found.map(_.organizationId) shouldBe Some("org-2")
  }

  test("getStorageProfile by orgId and instanceNum") {
    val cache = StorageProfileCacheFile.fromYamlString(testYaml)

    val result = cache.getStorageProfile("org-1", 1)
    result shouldBe defined
    result.get.bucket shouldBe "bucket-1"
  }

  test("getStorageProfile by orgId and collectorId and bucket") {
    val cache = StorageProfileCacheFile.fromYamlString(testYaml)

    val result = cache.getStorageProfile("org-1", "test-collector", "bucket-1")
    result shouldBe defined
    result.get.cloudProvider shouldBe "ceph"
  }

  test("getStorageProfilesByOrgId should return all matches") {
    val cache = StorageProfileCacheFile.fromYamlString(testYaml)

    val org1Profiles = cache.getStorageProfilesByOrgId("org-1")
    org1Profiles.map(_.bucket) should contain only "bucket-1"
  }

  test("reloadFromFile should work with a real file") {
    val tempFile = File.createTempFile("test", ".yaml")
    val writer = new PrintWriter(tempFile)
    try {
      writer.write(testYaml)
    } finally {
      writer.close()
    }

    val cache = StorageProfileCacheFile.fromFile(tempFile.getAbsolutePath)
    tempFile.delete()

    val result = cache.getStorageProfile("org-1", "test-collector", "bucket-1")
    result shouldBe defined
    result.get.cloudProvider shouldBe "ceph"
  }
}
