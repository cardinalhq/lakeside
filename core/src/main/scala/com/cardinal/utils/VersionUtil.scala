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

import java.io.InputStream
import java.util.Properties

object VersionUtil {
  private val VersionProperties = "/version.properties"

  def getVersion: String = {
    val properties = new Properties()
    val inputStream: Option[InputStream] = Option(getClass.getResourceAsStream(VersionProperties))

    inputStream match {
      case Some(stream) =>
        try {
          properties.load(stream)
          properties.getProperty("version", "unknown")
        } catch {
          case e: Exception =>
            e.printStackTrace()
            "unknown"
        } finally {
          stream.close()
        }
      case None => "unknown"
    }
  }
}
