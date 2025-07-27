package com.tglanz.spark.apps.simple

import com.tglanz.spark.apps.delta.SchemaEvolutionEvolved.Record
import com.tglanz.spark.shared.{ParseArgs, RandomString}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.collection.mutable.ArrayBuffer
import scala.util.Random;

object SimpleParquetCreate {
  private type Record = (Int, Int, Int, String, Float)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SimpleParquetCreate")
      .getOrCreate()

    import spark.implicits._

    // args
    val argMap = ParseArgs.simple(args)
    val path = argMap.getOrElse("path", "/tmp/simple.parquet")
    val batchSize = argMap.getOrElse("batch-size", "100000").toInt
    val numRows = argMap.getOrElse("rows", "1000000").toInt
    val numPartitions = argMap.getOrElse("partitions", "10").toInt
    val randomStringSize = argMap.getOrElse("random-string-size", "7").toInt
    val shouldExplain = argMap.getOrElse("explain", "false").equalsIgnoreCase("true")

    def flushBatch(seq: Seq[Record]): Unit = {
      seq
        .toDF("row", "batch", "partition", "random_string", "random_number")
        .write
        .mode("overwrite")
        .partitionBy("partition")
        .parquet(path)
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
  }
}