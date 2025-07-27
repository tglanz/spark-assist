package com.tglanz.spark.shared

import util.Random

object RandomString {
  private val source = "abcdefghijklmnopqrstuvwxyz"

  def next(length: Integer): String = {
    (0 until length)
      .map(_ => source.charAt(Math.abs(Random.nextInt) % source.length))
      .mkString("")
  }
}
