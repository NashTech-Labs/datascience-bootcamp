package com.knoldus.training

import com.knoldus.common.{AppConfig, KLogger}
import com.knoldus.spark.Transformers
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Model
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.rdd.RDD

import com.cloudera.sparkts._
import com.cloudera.sparkts.models.{ARIMA, ARIMAModel}

object Forecasting2 {
  def main(args: Array[String]): Unit = {

    val LOGGER: Logger = KLogger.getLogger(this.getClass)



    // Spark Demo
    val spark = SparkSession
      .builder()
      .appName("Forecasting")
      .config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()

    AppConfig.setSparkSession(spark)
    import spark.implicits._
    import com.knoldus.spark.UDFs.containsTulipsUDF


    spark.stop()
  }

  def readFeaturesFile(featureFile: String): DataFrame = {
    ???
  }

  def fillInMissingData(data: DataFrame): DataFrame = {
    ???
  }

  def getDifferences(data: DataFrame): DataFrame = {
    ???
  }

  def doJoins(data: DataFrame): DataFrame = {
    ???
  }

  def doLinearRegression(data: DataFrame): DataFrame = {
    ???
  }
}
