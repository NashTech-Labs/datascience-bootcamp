package com.knoldus.training

import com.knoldus.common.{AppConfig, KLogger}
import com.knoldus.spark.Transformers
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Properties

import com.knoldus.common.{AppConfig, KLogger}
import org.eclipse.rdf4j.repository.RepositoryConnection

import scala.annotation.tailrec
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.eclipse.rdf4j.model._
import org.eclipse.rdf4j.model.impl.{SimpleValueFactory, TreeModel}
import org.eclipse.rdf4j.model.util.ModelBuilder
import org.eclipse.rdf4j.model.vocabulary._
import org.eclipse.rdf4j.query
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}
import org.eclipse.rdf4j.repository.Repository
import org.eclipse.rdf4j.repository.http.HTTPRepository
import java.io._

import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.eclipse.rdf4j.query.QueryLanguage
import org.eclipse.rdf4j.repository.sail.SailRepository
import org.eclipse.rdf4j.sail.memory.MemoryStore
import org.apache.rya.accumulo.AccumuloRdfConfiguration
import org.apache.rya.accumulo.AccumuloRyaDAO
import org.apache.rya.rdftriplestore.{RdfCloudTripleStore, RyaSailRepository}
import org.apache.accumulo.core.client.AccumuloException
import org.apache.accumulo.core.client.AccumuloSecurityException
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.MutationsRejectedException
import org.apache.accumulo.core.client.TableExistsException
import org.apache.accumulo.core.client.TableNotFoundException
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

//import org.apache.any23.rdf.RDFUtils

import org.apache.any23.extractor.csv

object RdfModel {
  lazy val model=new impl.TreeModel()
}

object RdfValueFactory {
  val vf=SimpleValueFactory.getInstance()
}

object GetTriples {

  def main(args: Array[String]): Unit = {

    // Logging Demonstration
    val LOGGER: Logger = KLogger.getLogger(this.getClass)
    val age = 20
    LOGGER.info("Age " + age )
    LOGGER.warn("This is warning")

    val conf=new SparkConf().setMaster("local[2]").setAppName("RyaTriples")
    val sc=new SparkContext(conf)


    // Spark Demo
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()

    AppConfig.setSparkSession(spark)
    import spark.implicits._
    import com.knoldus.spark.UDFs.containsTulipsUDF

    val path="/home/jouko/dev/project/TrainingSprints/TrainingSprint8/RyaTriples/src/main/resources/Legacy_Purchase_Orders.csv"

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", true)
      .load(path)

    df.show(5, false)
    val newColumns=df.columns.map( x => x.replace(" ", "_"))
    val dfRenamed=df.toDF(newColumns: _*).withColumn("ROW_ID", functions.monotonically_increasing_id())
    dfRenamed.columns.foreach(println)

    val knoldus="http://www.knoldus.com/"

    val columnNames=dfRenamed.columns

    columnNames.foreach(println)

    val vf=SimpleValueFactory.getInstance()

    val columnIris=getColumnIris(knoldus, vf, columnNames)

    RdfModel.model

    dfRenamed.foreach( x => addToModel(x, columnIris) )



    val rddRows=sc.textFile(path)
      .map( x => x.split(",") )
    //rddRows.collect.foreach( x => println(x.mkString ))
    /*
    val rows=rddRows.collect

    val model=new impl.TreeModel()

    val knoldus="http://www.knoldus.com/"

    val vf=SimpleValueFactory.getInstance()

    //val rowType=vf.createIRI(knoldus, "row")
    //val recordType=vf.createIRI(knoldus, "RECORD_TYPE")
    //val purchaseOrderNumber=vf.createIRI(knoldus, "PURCHASE_ORDER_NUMBER")

    val columnIris=getColumnIris(knoldus, vf, rows(0))

    columnIris.foreach(println)

    val convertTo=getConvertTo

    for {i <- 1 until rows.length} {
      val row = vf.createIRI(knoldus, "row_" + i)
      model.add(row, RDF.TYPE, vf.createLiteral("order"))
      for {j <- columnIris.indices} {
        if (rows(i)(j)!="") {
          if (convertTo(j)=="Int") { model.add(row, columnIris(j), vf.createLiteral(rows(i)(j).toInt)) }
          else if (convertTo(j)=="Double") { model.add(row, columnIris(j), vf.createLiteral(rows(i)(j).toDouble)) }
          else { model.add(row, columnIris(j), vf.createLiteral(rows(i)(j))) }
          //model.add(row, columnIris(j), vf.createLiteral(rows(i)(j)))
        }
      }
    }

    */
    val outFile="test.ttl"

    writeModel(outFile, RdfModel.model)

    //for { i <- 0 to rows.size } {
    //  val rowSubject=RDFUtils.iri(documentIRI.toString(), "row/" + i)
    //}


    spark.stop()

  }

  def addToModel(row: Row, colNames: Array[IRI]): Unit = {

    //val model=new impl.TreeModel()

    val knoldus="http://www.knoldus.com/"

    //val convertTo=getConvertTo
    //println(row.schema.fields(0).dataType)

    val vf=SimpleValueFactory.getInstance()
    //println(row)

      val rowIRI = vf.createIRI(knoldus, "row_" + row.getLong(row.size-1))
      RdfModel.model.add(rowIRI, RDF.TYPE, vf.createLiteral("order"))
      for {j <- 0 until row.size-1} {
        if (row(j)!=null) {
          //if (convertTo(j)=="Int") { model.add(row, columnIris(j), vf.createLiteral(rows(i)(j).toInt)) }
          //else if (convertTo(j)=="Double") { model.add(row, columnIris(j), vf.createLiteral(rows(i)(j).toDouble)) }
          //else { model.add(row, columnIris(j), vf.createLiteral(rows(i)(j))) }
          //println(row.schema.fields(j).dataType)
          row.schema.fields(j).dataType match {
            //println(row.getString(j))
            case StringType => RdfModel.model.add(rowIRI, colNames(j), vf.createLiteral(row.getString(j)))
            case LongType => RdfModel.model.add(rowIRI, colNames(j), vf.createLiteral(row.getLong(j)))
            case DoubleType => RdfModel.model.add(rowIRI, colNames(j), vf.createLiteral(row.getDouble(j)))
            case IntegerType => RdfModel.model.add(rowIRI, colNames(j), vf.createLiteral(row.getInt(j)))
          }
        }
      }
  }

  def getConvertTo: Array[String] = {
    Array(
     "String",  //0
      "String", //1
      "String", //2
      "String", //3
      "Double", //4
      "Int",    //5
      "String", //6
      "Int",    //7
      "String", //8
      "String", //9
      "String", //10
      "String", //11
      "String", //12
      "String", //13
      "String", //14
      "Int",    //15
      "String", //16
      "Double", //17
      "Int",    //18
      "String", //19
      "String", //20
      "String", //21
      "String", //22
      "String", //23
      "String", //24
      //"Int",    //25
      "String", //25
      "String", //26
      "String", //27
      "String", //28
      "String", //29
      "String", //30
      "String", //31
      "Int",    //32
      "Double", //33
      "Int",    //34
      "String", //35
      "String", //36
      "String", //37
      "String", //38
      "String", //39
      "String", //40
      "String" //41
    )
  }

  def getColumnIrisRec(knoldus: String, vf: SimpleValueFactory, cols: Array[String], n: Int, result: Array[IRI]): Array[IRI] = {
    if (n==cols.length) { result }
    else { val iri= vf.createIRI(knoldus, cols(n).replace(" ", "_")); getColumnIrisRec(knoldus, vf, cols, n+1, result :+ iri ) }
  }

  def getColumnIris(knoldus: String, vf: SimpleValueFactory, cols: Array[String]): Array[IRI] = {
    val result: Array[IRI]=Array()
    getColumnIrisRec(knoldus, vf, cols, 0, result)
  }

  def writeModel(filename: String, model: Model): Unit = {
    val pw=new PrintWriter(new File(filename))
    Rio.write(model, pw, RDFFormat.TURTLE)
  }

}
