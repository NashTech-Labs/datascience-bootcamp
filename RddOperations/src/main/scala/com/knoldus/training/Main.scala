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

  def stringArrayToGdelt(words: Array[String]): GDELT = {
    println("numWords= "+words.length)
    println(words.mkString)
    val id=words(0).toInt
    val day=words(1).toInt
    val monthYear=words(2).toInt
    val year=words(3).toInt
    val fractionDate=words(4).toDouble

    val actor1Code=words(5)
    val actor1Name=words(6)
    val actor1CountryCode=words(7)
    val actor1KnownGroupCode=words(8)
    val actor1EthnicCode=words(9)
    val actor1Religion1Code=words(10)
    val actor1Religion2Code=words(11)
    val actor1Type1Code=words(12)
    val actor1Type2Code=words(13)
    val actor1Type3Code=words(14)

    val actor2Code=words(15)
    val actor2Name=words(16)
    val actor2CountryCode=words(17)
    val actor2KnownGroupCode=words(18)
    val actor2EthnicCode=words(19)
    val actor2Religion1Code=words(20)
    val actor2Religion2Code=words(21)
    val actor2Type1Code=words(22)
    val actor2Type2Code=words(23)
    val actor2Type3Code=words(24)

    val isRootEvent=words(25).toInt
    val eventCode=words(26)
    val eventBaseCode=words(27)
    val eventRootCode=words(28)
    val quadClass=words(29)
    val goldsteinScale=words(30).toDouble
    val numMentions=words(31).toInt
    val numSources=words(32).toInt
    val numArticles=words(33).toInt
    val avgTone=words(34).toDouble

    val actor1Geo_Type=strToInt(words(35))
    val actor1Geo_Fullname=words(36)
    val actor1Geo_CountryCode=words(37)
    val actor1Geo_ADM1Code=words(38)
    val actor1Geo_Lat=strToDouble(words(39))
    val actor1Geo_Long=strToDouble(words(40))
    val actor1Geo_FeatureID=strToInt(words(41))
    
    val actor2Geo_Type=strToInt(words(42))
    val actor2Geo_Fullname=words(43)
    val actor2Geo_CountryCode=words(44)
    val actor2Geo_ADM1Code=words(45)
    val actor2Geo_Lat=strToDouble(words(46))
    val actor2Geo_Long=strToDouble(words(47))
    val actor2Geo_FeatureID=strToInt(words(48))
     
    val actionGeo_Type=strToInt(words(49))
    val actionGeo_Fullname=words(50)
    val actionGeo_CountryCode=words(51)
    val actionGeo_ADM1Code=words(52)
    val actionGeo_Lat=strToDouble(words(53))
    val actionGeo_Long=strToDouble(words(54))
    val actionGeo_FeatureID=strToInt(words(55))

    val dateAdded=strToInt(words(56))
    val sourceUrl=words(57)


    GDELT(id, day, monthYear, year, fractionDate, 
      actor1Code, actor1Name, actor1CountryCode, actor1KnownGroupCode, actor1EthnicCode, actor1Religion1Code, actor1Religion2Code, actor1Type1Code, actor1Type2Code, actor1Type3Code,
      actor2Code, actor2Name, actor2CountryCode, actor2KnownGroupCode, actor2EthnicCode, actor2Religion1Code, actor2Religion2Code, actor2Type1Code, actor2Type2Code, actor2Type3Code,
      isRootEvent, eventCode, eventBaseCode, eventRootCode, quadClass, goldsteinScale, numMentions, numSources, numArticles, avgTone,
      actor1Geo_Type, actor1Geo_Fullname, actor1Geo_CountryCode, actor1Geo_ADM1Code, actor1Geo_Lat, actor1Geo_Long, actor1Geo_FeatureID,
      actor2Geo_Type, actor2Geo_Fullname, actor2Geo_CountryCode, actor2Geo_ADM1Code, actor2Geo_Lat, actor2Geo_Long, actor2Geo_FeatureID,
      actionGeo_Type, actionGeo_Fullname, actionGeo_CountryCode, actionGeo_ADM1Code, actionGeo_Lat, actionGeo_Long, actionGeo_FeatureID,
      dateAdded, sourceUrl)
    //GDELT(15, "asdf")
  }

  def isAllDigits(word: String): Boolean = {
    word forall Character.isDigit
  }

  def strToInt(word: String): Int = {
    if (word.trim=="" || !isAllDigits(word) ) { 0 }
    else { word.trim.toInt }
  }

  def strToDouble(word: String): Double = {
    if (word.trim=="") { 0.0 }
    else { println("word= "+word); word.trim.toDouble }
  }

}

case class GDELT(id: Int, day: Int, monthYear: Int, year: Int, fractionDate: Double, actor1Code: String, actor1Name: String, actor1CountryCode: String, actor1KnownGroupCode: String, actor1EthnicCode: String, actor1Religion1Code: String, actor1Religion2Code: String, actor1Type1Code: String, actor1Type2Code: String, actor1Type3Code: String, actor2Code: String, actor2Name: String, actor2CountryCode: String, actor2KnownGroupCode: String, actor2EthnicCode: String, actor2Religion1Code: String, actor2Religion2Code: String, actor2Type1Code: String, actor2Type2Code: String, actor2Type3Code: String, isRootEvent: Int, eventCode: String, eventBaseCode: String, eventRootCode: String, quadClass: String, goldsteinScale: Double, numMentions: Int, numSources: Int, numArticles: Int, avgTone: Double, actor1Geo_Type: Int, actor1Geo_Fullname: String, actor1Geo_CountryCode: String, actor1Geo_ADM1Code: String, actor1Geo_Lat: Double, actor1Geo_Long: Double, actor1Geo_FeatureID: Int, actor2Geo_Type: Int, actor2Geo_Fullname: String, actor2Geo_CountryCode: String, actor2Geo_ADM1Code: String, actor2Geo_Lat: Double, actor2Geo_Long: Double, actor2Geo_FeatureID: Int, actionGeo_Type: Int, actionGeo_Fullname: String, actionCountryCode: String, action_ADM1Code: String, actionGeo_Lat: Double, actionGeo_Long: Double, actionGeo_FeatureID: Int, dateAdded: Int, sourceUrl: String) {


  override def toString(): String = { "year: "+ year + " actor: " + actor1Code }
}
