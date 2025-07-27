package com.tglanz.spark.apps.delta

import com.tglanz.spark.shared.{ParseArgs, RandomString}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random;

object SchemaEvolutionEvolved {
  private type Record = (Int, Int, Int, String, Float)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("DeltaSchemaEvolutionEvolved")
      .getOrCreate()

    import spark.implicits._

    // args
    val argMap = ParseArgs.simple(args)
    val tablePath = argMap.getOrElse("table-path", "/tmp/delta-table")
    val batchSize = argMap.getOrElse("batch-size", "1000").toInt
    val numRows = argMap.getOrElse("rows", "100000").toInt
    val numPartitions = argMap.getOrElse("partitions", "10").toInt
    val randomStringSize = argMap.getOrElse("random-string-size", "7").toInt

    def flushBatch(seq: Seq[Record]): Unit = {
      seq
        .toDF("row", "batch", "partition", "random_string", "random_number")
        .write
        .option("mergeSchema", "true")
        .mode("append")
        .format("delta")
        .partitionBy("partition")
        .save(tablePath)
    }

    val batch = new ArrayBuffer[Record](initialSize = batchSize)

    (0 until numRows).foreach(row => {
      val partition = row % numPartitions
      val randomString = RandomString.next(randomStringSize)
      val randomNumber = Random.nextFloat()
      batch.append((row, row % batchSize, partition, randomString, randomNumber))
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