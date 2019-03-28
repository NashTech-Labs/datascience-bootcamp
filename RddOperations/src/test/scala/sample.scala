
import com.knoldus.common.AppConfig

import junit.framework.TestCase
import org.junit.Assert._
import org.junit.Test
import com.knoldus.training.Main
import scala.collection.mutable.ListBuffer

import org.apache.spark.{SparkConf, SparkContext}

import com.knoldus.training.Transformations

class ExampleSuite extends TestCase {

  var sb: StringBuilder = _
  var lb: ListBuffer[String] = _

  //val conf = new SparkConf().setMaster("local[2]").setAppName("RddOperations")
  //val sc = new SparkContext(conf)
  
  val sc: SparkContext = {
    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.broadcast.compress", "false")
    sparkConfig.set("spark.shuffle.compress", "false")
    sparkConfig.set("spark.shuffle.spill.comress", "false")
    sparkConfig.set("spark.driver.allowMultipleContexts", "true")
    new SparkContext("local[2]", "RddOperations", sparkConfig)
  }
  
  //val spark = AppConfig.sparkSession
  //import spark.implicits._

  override def setUp() :Unit = {
    sb = new StringBuilder("ScalaTest is ")
    lb = new ListBuffer[String]
    println("Setup")
  }

  @Test
  def testone: Unit = {
    assertEquals("1","1")
  }

  @Test
  def testSplitDelimeter(): Unit = {
    val rdd=sc.parallelize(List("Hi\tthere", "How\tare"))
    val split=Transformations.splitDelimeter(rdd).collect
    val result=List(List("Hi", "there"), List("How", "are"))
    val splitList=split.map( x => x.toList ).toList
    assertEquals(splitList, result)
  }

  override def tearDown(): Unit = {
    println("End")
    super.tearDown()
  }
}
