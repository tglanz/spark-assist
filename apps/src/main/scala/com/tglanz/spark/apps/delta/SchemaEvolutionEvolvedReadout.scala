package com.tglanz.spark.apps.delta

import com.tglanz.spark.shared.{ParseArgs}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SchemaEvolutionEvolvedReadout {
  private type Record = (Int, Int, Int, String, Float)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("DeltaSchemaEvolutionReadout")
      .getOrCreate()

    // args
    val argMap = ParseArgs.simple(args)
    val tablePath = argMap.getOrElse("table-path", "/tmp/delta-table")
    val outputPath = argMap.getOrElse("output-path", "/tmp/delta-table.readout")
    val randomStringStartsWith = argMap.getOrElse("random-string-starts-with", "")
    val maxRandomNumber = argMap.getOrElse("max-random-number", "0.5")
    val excludeNullRandomNumbers = argMap.getOrElse("exclude-null-random-numbers", "false").equalsIgnoreCase("true")

    var df = spark.read
      .format("delta")
      .load(tablePath)

    if (randomStringStartsWith.nonEmpty) {
      df = df.where(col("random_string").startsWith(randomStringStartsWith))
    }

    if (maxRandomNumber.nonEmpty) {
      df = df.filter(col("random_number") < maxRandomNumber.toFloat)
    }

    if (excludeNullRandomNumbers) {
      df = df.filter(col("random_number").isNotNull)
    }

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