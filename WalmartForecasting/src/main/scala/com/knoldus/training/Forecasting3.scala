package com.knoldus.training

import java.io.{File, PrintWriter}

import com.cloudera.sparkts.models.ARIMA
import com.knoldus.common.{AppConfig, KLogger}
import com.knoldus.common.Constants._
import com.knoldus.training.Forecasting.{ForecastDate, readTestFile}
import com.knoldus.training.Forecasting2._
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object Forecasting3 {
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

    val featureFiles = args(0)
    val trainingFile = args(1)
    val testFile = args(2)
    val regParam = args(3).toDouble
    val outputPath = args(4)

    makeForecast(featureFiles, trainingFile, testFile, regParam, outputPath, spark)

    spark.stop()
  }

  def makeForecast(featureFiles: String, trainingFile: String, testFile: String, regParam: Double, outputPath: String, spark: SparkSession): Unit = {
    val test = readTestFile(testFile)

    val files = spark.sparkContext.textFile(featureFiles)
    val features = files.map( file => readFeaturesFile(file))
    features.persist
    val paths = spark.sparkContext.textFile(trainingFile)
    val data = paths.map( x => Forecasting.readTrainingFile(x) )
    data.persist
    val joined = joinFeatures(features, data)
    joined.persist
    val filledIn = joined.map( x => fillMissingData(x) )
    filledIn.persist
    val collected = filledIn.collect

    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)

    val modeled = collected.map( x => doLinearRegression(x, regParam, sqlContext) )

    val reasonable = makeReasonable(modeled)

    val result = getPredictionsForTestDates(test, reasonable, spark)

    writeOutput(result, outputPath)
  }

  def fillMissingCpis(info: Array[Info]): Array[Info] = {
    val cpi = info.map( x => x.cpi ).filter( x => x!=UNKNOWN_DOUBLE )
    val ts = org.apache.spark.mllib.linalg.Vectors.dense(cpi)
    val model = ARIMA.autoFit(ts)
    val forecast = model.forecast(ts, info.length-cpi.length).toArray
    val newInfo = for { i <- cpi.length until info.length } yield {
      info(i).copy( cpi = forecast(i) )
    }
    val a = newInfo.toArray
    info.slice(0, cpi.length) ++ a
  }

  def fillMissingUnemployment(info: Array[Info]): Array[Info] = {
    val unemployment = info.map( x => x.unemployment ).filter( x => x!=UNKNOWN_DOUBLE )
    val ts = org.apache.spark.mllib.linalg.Vectors.dense(unemployment)
    val model = ARIMA.autoFit(ts)
    val forecast = model.forecast(ts, info.length-unemployment.length).toArray
    val newInfo = for { i <- unemployment.length until info.length } yield {
      info(i).copy( unemployment = forecast(i) )
    }
    val a = newInfo.toArray
    info.slice(0, unemployment.length) ++ a
  }

  def fillMissingMarkDowns(info: Array[Info]): Array[Info] = {
    //val zero: Array[Double] = Array(0, 0, 0, 0, 0)
    //info.map(x => if (x.markDowns(0)==UNKNOWN_DOUBLE) { x.copy(markDowns = zero) } else x)
    val ave = for { i <- 0 until 5 } yield {
      val notNull = info.map( x => x.markDowns(i) ).filter( x => x!=UNKNOWN_DOUBLE )
      notNull.sum/notNull.length.toDouble
    }
    info.map(x => if (x.markDowns(0)==UNKNOWN_DOUBLE) { x.copy(markDowns = ave.toArray ) } else x)
  }

  def fillMissingData(info: Array[Info]): Array[Info] = {
    val markDowns = fillMissingMarkDowns(info)
    val cpi = fillMissingCpis(markDowns)
    fillMissingUnemployment(cpi)
  }

  def applyModel(xy: Array[(Double, Vector)], info: Array[Info], intercept: Double, coefficients: Vector):
  Array[Info] = {
    val predicted = for { i <- info.indices } yield {
      if (info(i).sales!=UNKNOWN_DOUBLE) { info(i) }
      else {
        val dotProduct = for { j <- 0 until coefficients.size } yield {
          coefficients(j)*xy(i)._2(j)
        }
        val newSale = intercept + dotProduct.toArray.sum
        info(i).copy(sales = newSale, predicted = true)
      }
    }
    predicted.toArray
  }

  def getWeek(date: ForecastDate): Int = {
    (date.year*365 + date.month*31 + date.day)/7
  }

  def getWeekOfYear(date: ForecastDate): Int = {
    ((date.month-1)*31 + date.day)/7
  }

  def infoToDataPoint(info: Array[Info], weeklyAve: Array[Double], weeklyAveTemp: Array[Double], n: Int):
  (Double, Vector) = {
    val week = getWeekOfYear(info(n).date)
    val ave = if (week<weeklyAve.length) { weeklyAve(week) } else 0
    val aveTemp = if (week<weeklyAveTemp.length) { weeklyAveTemp(week)-info(n).temperature } else 0
    (info(n).sales, Vectors.dense(info(n).temperature, info(n).isHoliday, getWeek(info(n).date), info(n).fuelPrice,
      ave,
      aveTemp
      //info(n).markDowns(0), info(n).markDowns(1), info(n).markDowns(2), info(n).markDowns(3), info(n).markDowns(4),
      //info(n).cpi, info(n).unemployment
    ) )
  }

  def infoToDataPoint(info: Array[Info], aveSale: Double, n: Int): (Double, Vector) = {
    val week = getWeekOfYear(info(n).date)
    val weekVector: Array[Double] = Array.fill(week){0.0} ++ Array(1.0) ++ Array.fill(54-week-1){0.0} ++
    Array(info(n).temperature, info(n).isHoliday, getWeek(info(n).date), info(n).fuelPrice) ++ info(n).markDowns ++
    Array(info(n).cpi, info(n).unemployment)
    (info(n).sales/aveSale, Vectors.dense(weekVector))
  }

  def getWeeklyAverage(info: Array[Info]): Array[Double] = {
    val filtered = info.filter( x => x.sales>0 )
    val indexed = filtered.map( x => ( getWeekOfYear(x.date), x.sales ) )
    val grouped = indexed.groupBy( x => x._1 )
    val averaged = grouped.map( x => (x._1, x._2.map( y => y._2 ).sum/x._2.length ) )
    val result = averaged.toArray.sortBy( x => x._1 ).map( x => x._2 )
    //for { i <- result.indices } {
    //  println("store= " + info(0).store + " dept= " + info(0).dept + " i= " + i + " " + result(i) )
    //}
    result
  }

  def getWeeklyAverageTemperature(info: Array[Info]): Array[Double] = {
    val filtered = info.filter( x => x.sales>0 )
    val indexed = filtered.map( x => ( getWeekOfYear(x.date), x.temperature ) )
    val grouped = indexed.groupBy( x => x._1 )
    val averaged = grouped.map( x => (x._1, x._2.map( y => y._2 ).sum/x._2.length ) )
    val result = averaged.toArray.sortBy( x => x._1 ).map( x => x._2 )
    //for { i <- result.indices } {
    //  println("store= " + info(0).store + " dept= " + info(0).dept + " i= " + i + " " + result(i) )
    //}
    result
  }

  def writeComparison(info: Array[Info], newInfo: Array[Info]): Unit = {
    val outDirectory = "data/comparison/"
    val outFile = outDirectory + info(0).store + "_" + info(0).dept + ".csv"
    val pw = new PrintWriter(new File(outFile))
    for { i <- info.indices } {
      pw.write(i + "," + info(i).sales + "," + newInfo(i).sales + "\n")
    }
    pw.close()
  }

  def copyPredictionToInfo(df: DataFrame, aveSale: Double, info: Array[Info]): Array[Info] = {
    val predCol = df.select("prediction").collect
    val predicted = for { i <- info.indices } yield {
      if (info(i).sales != UNKNOWN_DOUBLE) { info(i) }
      else { info(i).copy(sales = predCol(i).getDouble(0)*aveSale, predicted = true ) }
    }
    predicted.toArray
  }

  def doLinearRegression(info: Array[Info], regParam: Double, sqlContext: SQLContext): Array[Info] = {

    val weeklyAve = getWeeklyAverage(info)
    val weeklyAveTemp = getWeeklyAverageTemperature(info)

    val filteredSales = info.map( x => x.sales ).filter( x => x>UNKNOWN_DOUBLE )
    val aveSale = filteredSales.sum/filteredSales.length.toDouble
    val xy = for { i <- info.indices } yield {
      //infoToDataPoint(info, weeklyAve, weeklyAveTemp, i)
      //println("store= " + info(i).store + " dept= " + info(i).dept + " cpi " + info(i).cpi + " " + info(i).unemployment)
      infoToDataPoint(info, aveSale, i)
    }

    //for { i <- xy.indices } {
    //  println("store= " + info(i).store + " dept= " + info(i).dept + " " + xy(i)._1 + " " + xy(i)._2.toArray.mkString(" "))
    //}

    import sqlContext.implicits._

    //val df=xy.filter( x => x._1>UNKNOWN_DOUBLE && !x._1.isNaN && !containsNan(x._2) ).toDF("label", "unscaledFeatures")
    val df=xy.filter( x => x._1>0 && !x._1.isNaN && !containsNan(x._2) ).toDF("label", "unscaledFeatures")
    val dfAll=xy.toDF("label", "unscaledFeatures")

    val scaler = new StandardScaler()
      .setInputCol("unscaledFeatures")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)

    val lr = new LinearRegression().setRegParam(regParam)

    val pipeline = new Pipeline().setStages(Array(scaler, lr))
    val model = pipeline.fit(df)
    val prediction = model.transform(dfAll)
    val newInfo = copyPredictionToInfo(prediction, aveSale, info)


    writeComparison(info, newInfo)

    newInfo
  }

}
