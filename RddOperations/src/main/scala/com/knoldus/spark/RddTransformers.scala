package com.knoldus.training

import com.knoldus.common.{AppConfig, KLogger}
import com.knoldus.spark.Transformers
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Transformations {

  val spark = AppConfig.sparkSession
  import spark.implicits._

  def splitDelimeter(rdd: RDD[String]): RDD[Array[String]] = {
    rdd.map( line => line.split("\t") )
  }

  def getYearActor(rdd: RDD[Array[String]]): RDD[(Int, String)] = {
    rdd.map( x => (x(3).toInt, x(5)) )
  }

  def getActor(rdd: RDD[Array[String]]): RDD[String] = {
    rdd.map( x => x(5) )
  }

  def getIdActor(rdd: RDD[Array[String]]): RDD[(Int, String)] = {
    rdd.map( x => (x(0).toInt, x(5)) )
  }

  def getGdelRdd(rdd: RDD[String]): RDD[GDELT] = {
    val rddSplit=splitDelimeter(rdd)
    rddSplit.map( x => Main.stringArrayToGdelt(x) )
  }

  def aggregateExample(rdd: RDD[(Int, String)]): Unit = {
    val rdd2=rdd.map( x => x._1.toDouble )
    val sum=rdd2.aggregate(0.0)(_ + _, _ + math.log(_))
    println("sum= "+sum)
  }

  def aggregateByKeyExample(rdd: RDD[(Int, String)]): Unit = {
    val rdd2=rdd.map( x => (x._1, 1) )
    val concats=rdd2.aggregateByKey(0)(_ + _, _ + _).collect()
    concats.foreach(println)
  }

  //def rddHead[T](rdd: RDD[T], n: Int, sc: SparkContext): RDD[T] = {
  //  sc.parallelize(rdd.take(n))
  //}

  def cartesianExample(rdd: RDD[(Int, String)], sc: SparkContext): Unit = {
    //val rdd2=rddHead(rdd, 10, sc)
    val rdd2=sc.parallelize(rdd.take(10))
    val car=rdd2.cartesian(rdd2).collect
    car.foreach(println)
  }

  def checkpointExample(rdd: RDD[(Int, String)], checkpointDir: String, sc: SparkContext): Unit = {
    sc.setCheckpointDir(checkpointDir)
    rdd.checkpoint
    println("count= "+rdd.count)
  }

  def coalesceExample(rdd: RDD[(Int, String)]): Unit = {
    println("Number of partitions= "+rdd.partitions.length)
    val rdd2=rdd.coalesce(2, false)
    println("Number of partitions= "+rdd2.partitions.length)
  }

  def cogroupExample(rdd: RDD[(Int, String)], sc: SparkContext): Unit = {
    //val rdd2=rddHead(rdd, 10, sc)
    val rdd2=sc.parallelize(rdd.take(10))
    val grouped=rdd2.cogroup(rdd2).collect
    grouped.foreach(println)
  }

  def collectAsMapExample(rdd: RDD[(Int, String)]): Unit = {
    val collectedMap=rdd.collectAsMap
    println(collectedMap)
  }

  def combineByKeyExample(rdd: RDD[(Int, String)]): Unit = {
    val combined=rdd.combineByKey(List(_), (x: List[String], y: String) => y::x, (x:List[String], y: List[String]) => x:::y ).collect
    //println(combined.mkString)
  }

  def countApproxDistinctExample[T](rdd: RDD[T]): Unit = {
    val approxDistinct=rdd.countApproxDistinct(0.05)
    println("approxDistinct= " + approxDistinct)
  }

  def countApproxDistinctByKeyExample(rdd: RDD[(Int, String)]): RDD[(Int, Long)] = {
    rdd.countApproxDistinctByKey(0.05)
  }

  def countByKeyExample(rdd: RDD[(Int, String)]): scala.collection.Map[Int, Long] = {
    rdd.countByKey
  }

  def countByValueExample(rdd: RDD[(Int, String)]): scala.collection.Map[(Int, String), Long] = {
    rdd.countByValue
  }

  def dependenciesExample[T](rdd: RDD[T]): Seq[org.apache.spark.Dependency[_]] = {
    rdd.dependencies
  }

  def distinctExample(rdd: RDD[String]): RDD[String] = {
    rdd.distinct
  }

  def filterExample(rdd: RDD[(Int, String)]): RDD[(Int, String)] = {
    rdd.filter( x => x._2.length==4 )
  }

  def filterByRangeExample(rdd: RDD[(Int, String)]): RDD[(Int, String)] = {
    val rddSorted=sortByKeyExample(rdd)
    rddSorted.filterByRange(833246251, 833246271)
  }

  def sortByKeyExample(rdd: RDD[(Int, String)]): RDD[(Int, String)] = {
    rdd.sortByKey()
  }
}
