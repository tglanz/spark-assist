package com.tglanz.spark.apps.uty

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object IcebergAvroIceberg {
  def extract(spark: SparkSession, inputTablePath: String): DataFrame = {
    spark.sql(s"SELECT * FROM $inputTablePath")
  }

  def transform(df: DataFrame): DataFrame = {
   df
      .withColumn("processing_timestamp", current_timestamp())
    // .withColumn("processing_day", to_date(col("processing_timestamp")))
    // .withColumn("event_type",
    //   when(col("datapipeEvent").isNotNull,
    //     // Convert bytes to string to extract event type
    //     regexp_extract(
    //       expr("cast(datapipeEvent as string)"),
    //       "([A-Z_]+)_EVENT",
    //       1
    //     )
    //   ).otherwise("UNKNOWN")
    // )
    // .withColumn("user_segment",
    //   when(col("sonicNewUser") === true, "NEW_USER")
    //   .when(col("sonicUserID").isNotNull, "RETURNING_USER")
    //   .otherwise("ANONYMOUS")
    // )
    // .withColumn("has_plumbus_data",
    //   col("usersPlumbus").isNotNull ||
    //   col("serveDataPlumbus").isNotNull ||
    //   col("supplyPlumbus").isNotNull ||
    //   col("dsReqPlumbus").isNotNull ||
    //   col("dsOppPlumbus").isNotNull ||
    //   col("bidderPlumbus").isNotNull ||
    //   col("gatewayPlumbus").isNotNull
    // )
    // .withColumn("plumbus_count",
    //   coalesce(size(col("usersPlumbus")), lit(0)) +
    //   coalesce(size(col("serveDataPlumbus")), lit(0)) +
    //   coalesce(size(col("supplyPlumbus")), lit(0)) +
    //   coalesce(size(col("dsReqPlumbus")), lit(0)) +
    //   coalesce(size(col("dsOppPlumbus")), lit(0)) +
    //   coalesce(size(col("bidderPlumbus")), lit(0)) +
    //   coalesce(size(col("gatewayPlumbus")), lit(0))
    // )

  }

  def load(df: DataFrame, outputTablePath: String): Unit = {
    val writer = df
      .write
      .format("iceberg")
      .mode("append")
      .option("write-format", "avro")

    writer
      .saveAsTable(outputTablePath)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IcebergAvroIceberg")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg.warehouse", "file:///tmp/iceberg-warehouse")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.catalog.iceberg.type", "hadoop")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputDatabase = spark.conf.getOption("spark.iceberg.input.database")
      .orElse(sys.env.get("ICEBERG_INPUT_DATABASE"))
      .getOrElse("raw_data_sources_dev")

    val inputTable = spark.conf.getOption("spark.iceberg.input.table")
      .orElse(sys.env.get("ICEBERG_INPUT_TABLE"))
      .getOrElse("avro_input_data")

    val outputDatabase = spark.conf.getOption("spark.iceberg.output.database")
      .orElse(sys.env.get("ICEBERG_OUTPUT_DATABASE"))
      .getOrElse("raw_data_sources_dev")

    val outputTable = spark.conf.getOption("spark.iceberg.output.table")
      .orElse(sys.env.get("ICEBERG_OUTPUT_TABLE"))
      .getOrElse("processed_output_data")

    val inputTablePath = s"iceberg.$inputDatabase.$inputTable"
    val outputTablePath = s"iceberg.$outputDatabase.$outputTable"

    spark.sql(s"CREATE DATABASE IF NOT EXISTS iceberg.$outputDatabase")
    spark.sql(s"DROP TABLE IF EXISTS $outputTablePath")

    val createOutputTableSQL = s"""
      CREATE TABLE IF NOT EXISTS $outputTablePath (
        datapipeEvent BINARY,
        sonicUserID STRING,
        deviceFirstAppearanceTimestamp STRING,
        sonicNewUser BOOLEAN,
        usersPlumbus MAP<STRING, STRING>,
        serveDataPlumbus MAP<STRING, STRING>,
        supplyPlumbus MAP<STRING, STRING>,
        dsReqPlumbus MAP<STRING, STRING>,
        dsOppPlumbus MAP<STRING, STRING>,
        bidderPlumbus MAP<STRING, STRING>,
        gatewayPlumbus MAP<STRING, STRING>,
        ingestion_timestamp TIMESTAMP,
        ingestion_day DATE,
        hour INT,
        processing_timestamp TIMESTAMP
      )
      USING iceberg
      TBLPROPERTIES (
        'write.format.default' = 'avro'
      )
    """

    spark.sql(createOutputTableSQL)

    val inputDF = extract(spark, inputTablePath)
    val transformedDF = transform(inputDF)
    load(transformedDF, outputTablePath)

    spark.stop()
  }
}