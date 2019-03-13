package com.knoldus.training



import com.knoldus.common.{AppConfig, KLogger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, RangePartitioner}


case class Order(id: Int, supplier: String, part :Int)

class ExactPartitioner[V](partitions: Int, elements: Int) extends Partitioner {
  override val numPartitions = partitions
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    val part = (k-1) * partitions / elements
    println("Key:" + k + " Partition:" + part + " Elements:" + elements)
    part
  }
}

object SafeFileSample {


  def main(args: Array[String]): Unit = {

    // Spark Demo
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master("spark://localhost:7077")
      .getOrCreate()

    AppConfig.setSparkSession(spark)



    def getOrder(i: Int): Order = {
      val rand = (new RandomDataGenerator).getRandomGenerator
      val id = rand.nextInt(10000)
      val part = id / 100
      new Order(id, "supp" + id ,  part)
    }

    import spark.implicits._
    val rdd  :RDD[Order] = spark.sparkContext.makeRDD((1 to 10000)).map(s=> (s,getOrder(s)))
      .partitionBy(new ExactPartitioner[Int](10, 10000  )).map(s=> s._2)

    val df = rdd.toDF()
    df.show()
      df.write.mode("overwrite").parquet("/tmp/sfs/")
    spark.stop()

  }
}
