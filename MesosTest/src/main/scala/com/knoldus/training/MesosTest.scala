package com.knoldus.training

import com.knoldus.common.{AppConfig, KLogger}
import com.knoldus.spark.Transformers
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Sample {


  def main(args: Array[String]):Unit = {

    // Logging Demonstration
    val LOGGER: Logger = KLogger.getLogger(this.getClass)

    val master="mesos://192.168.1.5:5050"
    //val master="mesos://zk://192.168.1.5:2181,192.168.1.3:2181"

    val conf = new SparkConf().setMaster(master).setAppName("MesosTest")//.set("spark.master.rest.enabled", "true")
    val sc = new SparkContext(conf)

    // Spark Demo
    val spark = SparkSession
      .builder()
      .appName("MesosTest")
      //.set("spark.executor.uri", "http://192.168.1.5/spark-2.4.0-bin-hadoop2.7.tgz")
      .config("spark.executor.uri", "http://192.168.1.5/spark-2.4.0-bin-hadoop2.7.tgz")
      //.config("spark.master.rest.enabled", "true")
      .master(master)
      .getOrCreate()


    AppConfig.setSparkSession(spark)
    import spark.implicits._
    import com.knoldus.spark.UDFs.containsTulipsUDF

    val rdd=sc.parallelize( 1 to 1000000)

    val rdd2=rdd.map{ x => x*x }

    rdd2.collect

    val sum=rdd2.sum

    LOGGER.info("sum= " + sum)

    spark.stop()

  }


}
