package com.knoldus.training

import com.knoldus.common.{AppConfig, KLogger}
import com.knoldus.spark.Transformers
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Model
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.rdd.RDD

object Forecasting {


  def main(args: Array[String]):Unit = {

    // Logging Demonstration
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

    val pathFile=args(0)
    val outputPath=args(1)

    val paths=spark.sparkContext.textFile(pathFile)

    val data=paths.map( x => readFile(x) )
    val rawPredictions=data.map( x => doArima(x) )
    val models=calcLinearRegressionModel(rawPredictions)
    val predictions=applyLinearRegression(rawPredictions, models)
    writeOutputCsv(predictions, outputPath)

    spark.stop()

  }

  def readFile(file: String): Array[(Int, Int, Int, Double, Int)] = {
    ???
  }

  def doArima(data: Array[(Int, Int, Int, Double, Int)]): Array[(Int, Double, Int)] = {
    ???
  }

  def calcLinearRegressionModel(data: RDD[Array[(Int, Double, Int)]]): Array[LinearRegressionModel] = {
    ???
  }

  def applyLinearRegression(data: RDD[Array[(Int, Double, Int)]], models: Array[LinearRegressionModel]):
  Array[(Int, Double)] =
  {
    ???
  }

  def writeOutputCsv(prediction: Array[(Int, Double)], path: String): Unit =
  {
    ???
  }

}
