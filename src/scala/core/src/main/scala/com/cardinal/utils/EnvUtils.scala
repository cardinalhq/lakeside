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
