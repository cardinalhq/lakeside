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

object EnvUtils {
  def firstEnv(names: Seq[String], default: String = ""): String =
    names
      .collectFirst(Function.unlift(sys.env.get))
      .getOrElse(default)

  def mustFirstEnv(names: Seq[String]): String =
    names
      .collectFirst(Function.unlift(sys.env.get))
      .getOrElse(throw new RuntimeException(s"None of the environment variables ${names.mkString(", ")} are set"))

  def get(name: String, default: String = ""): String =
    sys.env.getOrElse(name, default)

  def mustGet(name: String): String =
    sys.env.getOrElse(name, throw new RuntimeException(s"Environment variable '$name' is not set"))

  /** Parse a comma-separated "k=v,k2=v2" string into a Map */
  def parseLabels(sel: String): Map[String, String] =
    sel.split(',').iterator
      .map(_.trim)
      .flatMap { kv =>
        kv.split("=", 2) match {
          case Array(k,v) if k.nonEmpty && v.nonEmpty => Some(k->v)
          case _                                       => None
        }
      }.toMap
}
