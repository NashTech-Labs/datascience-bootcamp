package com.knoldus.training

import java.io.{File, PrintWriter}

import com.knoldus.common.AppConfig
import org.apache.spark.sql.SparkSession


object SplitTrainingTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()

    AppConfig.setSparkSession(spark)
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val inputPath = args(0)
    val testSetSize = args(1).toDouble
    val trainingOutPath = args(1)
    val testOutPath = args(2)

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", true)
      .load(inputPath)

    val count=df.count()
    val fraction=testSetSize/count.toDouble

    val Array(trainingDf, testDf) = df.randomSplit(Array(1.0-fraction, fraction))

    trainingDf.write.format("com.databricks.spark.csv").option("header", "true").save(trainingOutPath)
    testDf.write.format("com.databricks.spark.csv").option("header", "true").save(testOutPath)
  }
}
