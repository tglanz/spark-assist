package com.tglanz.spark.apps.simple

import com.tglanz.spark.shared.{ParseArgs}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SimpleParquetReadout {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SimpleParquetReadout")
      .getOrCreate()

    // args
    val argMap = ParseArgs.simple(args)
    val path = argMap.getOrElse("path", "/tmp/simple.parquet")
    val outputPath = argMap.getOrElse("output-path", "/tmp/simple-parquet.readout")
    val randomStringStartsWith =  argMap.getOrElse("random-string-starts-with", "")
    val maxRandomNumber = argMap.getOrElse("max-random-number", "0.5")
    val excludeNullRandomNumbers = argMap.getOrElse("exclude-null-random-numbers", "false").equalsIgnoreCase("true")
    val shouldExplain = argMap.getOrElse("explain", "false").equalsIgnoreCase("true")
    val shouldCollect = argMap.getOrElse("collect", "false").equalsIgnoreCase("true")
    val shouldWrite = argMap.getOrElse("write", "true").equalsIgnoreCase("true")

    var df = spark.read.parquet(path)

    if (randomStringStartsWith.nonEmpty) {
      df = df.where(col("random_string").startsWith(randomStringStartsWith))
    }

    if (maxRandomNumber.nonEmpty) {
      df = df.filter(col("random_number") < maxRandomNumber.toFloat)
    }

    if (excludeNullRandomNumbers) {
      df = df.filter(col("random_number").isNotNull)
    }


    if (shouldExplain) {
      df.explain()
    }

    if (shouldCollect) {
      df.collect()
    }

    if (shouldWrite) {
      df.sort("row")
        .coalesce(1)
        .write
        .option("header", "true")
        .option("delimiter", ",")
        .mode("overwrite")
        .format("csv")
        .save(outputPath)
    }
  }
}