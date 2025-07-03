package com.cardinal.utils

import org.junit.jupiter.api.Test

class StringsTest {

  @Test
  def testCamelToSnakeCase(): Unit = {
    assert(Strings.camelToSnakeCase("CPUUtilization") == "cpuutilization")
    assert(Strings.camelToSnakeCase("CPUCreditUsage") == "cpucredit_usage")
    assert(Strings.camelToSnakeCase("DiskReadBytes") == "disk_read_bytes")
  }
}
