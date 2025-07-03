package com.cardinal.utils

object EnvUtils {
  def firstEnv(envars: Seq[String], default: String = ""): String =
    envars
      .collectFirst(Function.unlift(sys.env.get))
      .getOrElse(default)
}