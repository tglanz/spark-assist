package com.tglanz.spark.apps.delta

import com.tglanz.spark.shared.{ParseArgs, RandomString}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.collection.mutable.ArrayBuffer
import scala.util.Random;

object SchemaEvolutionDelete {
  private type Record = (Int, Int, Int, String, Float)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("DeltaSchemaEvolutionDelete")
      .getOrCreate()

    import spark.implicits._

    // args
    val argMap = ParseArgs.simple(args)
    val tablePath = argMap.getOrElse("table-path", "/tmp/delta-table")
    val deleteRatio = argMap.getOrElse("delete-ratio", "0.1").toFloat

    val df = spark.read
      .format("delta")
      .load(tablePath)

    val randomStringsToDelete = df.sample(deleteRatio)
      .select("random_string")
      .distinct()
      .collect()
      .map(_.getString(0))

    DeltaTable.forPath(tablePath)
      .delete(col("random_string").isin(randomStringsToDelete: _*))
  }
}