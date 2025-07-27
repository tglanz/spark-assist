package com.tglanz.spark.apps.delta

import com.tglanz.spark.shared.{ParseArgs, RandomString}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, ListBuffer};

object SchemaEvolutionInitial {
  private type Record = (Int, Int, Int, String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("DeltaSchemaEvolutionInitial")
      .getOrCreate()

    import spark.implicits._

    // args
    val argMap = ParseArgs.simple(args)
    val tablePath = argMap.getOrElse("table-path", "/tmp/delta-table")
    val batchSize = argMap.getOrElse("batch-size", "1000").toInt
    val numRows = argMap.getOrElse("rows", "100000").toInt
    val numPartitions = argMap.getOrElse("partitions", "10").toInt
    val randomStringSize = argMap.getOrElse("random-string-size", "7").toInt
    val shouldOverwrite = argMap.getOrElse("overwrite", "false").equalsIgnoreCase("true")

    // state
    var isFirstBatch = true

    def flushBatch(seq: Seq[Record]): Unit = {
      seq
        .toDF("row", "batch", "partition", "random_string")
        .write
        .mode(if (shouldOverwrite && isFirstBatch) "overwrite" else "append")
        .format("delta")
        .partitionBy("partition")
        .save(tablePath)
      isFirstBatch = false
    }

    val batch = new ArrayBuffer[Record](initialSize = batchSize)

    (0 until numRows).foreach(row => {
      val partition = row % numPartitions
      val randomString = RandomString.next(randomStringSize)
      batch.append((row, row % batchSize, partition, randomString))
      if (batch.size == batchSize) {
        flushBatch(batch)
        batch.clear()
      }
    })

    if (batch.nonEmpty) {
      flushBatch(batch)
    }

    spark.read
      .format("delta")
      .load(tablePath)
      .show()
  }
}