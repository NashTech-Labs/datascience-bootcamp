package com.knoldus.training

import com.knoldus.common.{AppConfig, KLogger}
import com.knoldus.spark.Transformers
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

import com.knoldus.training.Transformations

object Main {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("RddOperations")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //import sqlContext.implicits._
    //import sqlContext.createSchemaRDD

    // Logging Demonstration
    val LOGGER: Logger = KLogger.getLogger(this.getClass)
    val age = 20
    LOGGER.info("Age " + age )
    LOGGER.warn("This is warning")


    // Spark Demo
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    AppConfig.setSparkSession(spark)
    import spark.implicits._
    import com.knoldus.spark.UDFs.containsTulipsUDF


    val homeDir=sys.env("HOME")
    val projectDir=homeDir+"/dev/projects/TrainingSprints/TrainingSprint5/RddOperations"
    val path=projectDir+"/data/20190325.export.CSV"

    val checkpointDir=projectDir+"/Checkpoint"

    val rdd=sc.textFile(path)

    //val schemardd=rdd.map( line => line.split("\t") ).map( x => new GDELT(x(0).toInt, x(5)) )
    val rddGdel=Transformations.getGdelRdd(rdd)
    println(rdd.count)
    //val rddSplit2=rdd.transform(Transformations.splitDelimeter2())
    val rddSplit=Transformations.splitDelimeter(rdd)
    val yearActor=Transformations.getYearActor(rddSplit)
    val actor=Transformations.getActor(rddSplit)
    val idActor=Transformations.getIdActor(rddSplit)

    rddSplit.take(10).foreach(x => println(x(0)))
    /*
    yearActor.take(10).foreach(println)
    Transformations.aggregateExample(yearActor)
    Transformations.aggregateByKeyExample(yearActor)
    Transformations.cartesianExample(yearActor, sc)
    Transformations.checkpointExample(yearActor, checkpointDir, sc)
    Transformations.coalesceExample(yearActor)
    Transformations.cogroupExample(yearActor, sc)
    Transformations.collectAsMapExample(yearActor)
    Transformations.combineByKeyExample(yearActor)
    println(rdd.context)
    Transformations.countApproxDistinctExample(yearActor)

    val approxDistinctByKey=Transformations.countApproxDistinctByKeyExample(yearActor)
    println("approxDistinctByKey= "+approxDistinctByKey.collect.mkString)

    val countedByKey=Transformations.countByKeyExample(yearActor)
    println("countedByKey= "+countedByKey)

    val countedByValue=Transformations.countByValueExample(yearActor)
    //println(countedByValue)

    val dependencies=Transformations.dependenciesExample(yearActor)
    println("dependencies= "+dependencies.mkString)

    val distinctActors=Transformations.distinctExample(actor)
    distinctActors.take(10).foreach(println)

    //println("firstActor= "+actor.first)
    println("firstId= "+idActor.first)

    val len4Actors=Transformations.filterExample(yearActor)
    len4Actors.take(10).foreach(println)
    */
    rddGdel.take(10).foreach(println)
    rddSplit.take(10).foreach(x => println(x(25)))

    spark.stop()

  }
}
