package com.tglanz.spark.delta

import org.apache.spark.sql.SparkSession;

object SchemaEvolutionB {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("SchemaEvolution")
      .getOrCreate()

    import spark.implicits._

    Seq(
      ("tal", 35, "israel"),
    )
      .toDF("first_name", "age", "country")
      .write.option("mergeSchema", "true").mode("append").format("delta").save("/tmp/fun_people")

    spark.read.format("delta").load("/tmp/fun_people").show()
  }
}