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

import com.cardinal.logs.LogCommons.{DATA_SIZE_TYPE, DURATION_TYPE}
import org.junit.jupiter.api.Test

class QuantityParserTest {

  @Test
  def testParseQuantityDuration(): Unit = {
    // Test duration parsing
    assert(QuantityParser.parseQuantity("30s", DURATION_TYPE) == Some(30000000000.0)) // 30 seconds to nanos
    assert(QuantityParser.parseQuantity("2m", DURATION_TYPE) == Some(120000000000.0)) // 2 minutes to nanos
    assert(QuantityParser.parseQuantity("1h", DURATION_TYPE) == Some(3600000000000.0)) // 1 hour to nanos
    
    // Test invalid duration
    assert(QuantityParser.parseQuantity("invalid", DURATION_TYPE) == None)
  }

  @Test
  def testParseQuantityDataSize(): Unit = {
    // Test data size parsing
    assert(QuantityParser.parseQuantity("100b", DATA_SIZE_TYPE) == Some(100.0)) // 100 bytes
    assert(QuantityParser.parseQuantity("2kb", DATA_SIZE_TYPE) == Some(2000.0)) // 2 kilobytes to bytes
    assert(QuantityParser.parseQuantity("1mb", DATA_SIZE_TYPE) == Some(1000000.0)) // 1 megabyte to bytes
    
    // Test invalid size
    assert(QuantityParser.parseQuantity("invalid", DATA_SIZE_TYPE) == None)
  }

  @Test
  def testParseQuantityInvalidInput(): Unit = {
    // Test with invalid input formats
    assert(QuantityParser.parseQuantity("", DURATION_TYPE) == None)
    assert(QuantityParser.parseQuantity("nounit", DURATION_TYPE) == None)
    assert(QuantityParser.parseQuantity("123", DATA_SIZE_TYPE) == None) // missing unit
  }
}