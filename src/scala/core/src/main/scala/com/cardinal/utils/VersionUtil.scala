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
