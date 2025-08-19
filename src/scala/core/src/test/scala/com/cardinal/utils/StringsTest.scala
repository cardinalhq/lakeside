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

package com.cardinal.utils

import org.junit.jupiter.api.Test
import java.time.Duration

class StringsTest {

  @Test
  def testCamelToSnakeCase(): Unit = {
    assert(Strings.camelToSnakeCase("CPUUtilization") == "cpuutilization")
    assert(Strings.camelToSnakeCase("CPUCreditUsage") == "cpucredit_usage")
    assert(Strings.camelToSnakeCase("DiskReadBytes") == "disk_read_bytes")
  }

  @Test
  def testToString(): Unit = {
    // Test duration to string conversion
    assert(Strings.toString(Duration.ofSeconds(30)) == "30s")
    assert(Strings.toString(Duration.ofMinutes(5)) == "5m")
    assert(Strings.toString(Duration.ofHours(2)) == "2h")
    assert(Strings.toString(Duration.ofDays(3)) == "3d")
    assert(Strings.toString(Duration.ofDays(14)) == "2w")
    
    // Test non-standard durations that fall back to default toString
    assert(Strings.toString(Duration.ofMillis(1500)) == "PT1.5S")
  }

  @Test
  def testStripMargin(): Unit = {
    // Test basic margin stripping with single newlines converted to spaces
    val input1 = """
      |This is a test
      |with multiple lines
      |that should be cleaned up""".stripMargin
    val expected1 = "This is a test with multiple lines that should be cleaned up"
    assert(Strings.stripMargin(input1) == expected1)

    // Test double newlines preserved as paragraph breaks
    val input2 = """
      |First paragraph
      |continues here
      |
      |
      |Second paragraph
      |also continues""".stripMargin
    val expected2 = "First paragraph continues here\n\nSecond paragraph also continues"
    assert(Strings.stripMargin(input2) == expected2)

    // Test simple string without margins
    val input3 = "Simple string"
    val expected3 = "Simple string"
    assert(Strings.stripMargin(input3) == expected3)
  }
}
