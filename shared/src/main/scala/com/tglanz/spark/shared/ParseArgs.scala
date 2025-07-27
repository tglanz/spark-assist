package com.tglanz.spark.shared

object ParseArgs {
  def simple(args: Array[String]): Map[String, String] = {
    args.dropWhile(v => v == "--").sliding(2, 2).collect {
      case Array(flag, value) if flag.startsWith("--") => flag.drop(2) -> value
    }.toMap
  }
}
