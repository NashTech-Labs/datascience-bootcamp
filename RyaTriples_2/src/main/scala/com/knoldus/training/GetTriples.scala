package com.knoldus.training

import com.knoldus.common.{AppConfig, KLogger}
import com.knoldus.spark.Transformers
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Properties

import com.knoldus.common.{AppConfig, KLogger}
import org.eclipse.rdf4j.repository.RepositoryConnection

import scala.annotation.tailrec
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
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

//import org.apache.any23.rdf.RDFUtils

import org.apache.any23.extractor.csv


object GetTriples {

  def main(args: Array[String]): Unit = {

    // Logging Demonstration
    val LOGGER: Logger = KLogger.getLogger(this.getClass)
    val age = 20
    LOGGER.info("Age " + age )
    LOGGER.warn("This is warning")

    val conf=new SparkConf().setMaster("local[2]").setAppName("RyaTriples")
    val sc=new SparkContext(conf)
    /*

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
    */
    val rddRows=sc.textFile("/home/jouko/dev/project/TrainingSprints/TrainingSprint8/RyaTriples/src/main/resources/Legacy_Purchase_Orders.csv")
      .map( x => x.split(",") )
    rddRows.collect.foreach( x => println(x.mkString ))

    val rows=rddRows.collect

    val model=new impl.TreeModel()

    val knoldus="http://www.knoldus.com/"

    val vf=SimpleValueFactory.getInstance()

    //val rowType=vf.createIRI(knoldus, "row")
    //val recordType=vf.createIRI(knoldus, "RECORD_TYPE")
    //val purchaseOrderNumber=vf.createIRI(knoldus, "PURCHASE_ORDER_NUMBER")

    val columnIris=getColumnIris(knoldus, vf, rows(0))

    columnIris.foreach(println)

    for {i <- 1 until rows.length} {
      val row = vf.createIRI(knoldus, "row_" + i)
      model.add(row, RDF.TYPE, vf.createLiteral("order"))
      for {j <- columnIris.indices} {
        if (rows(i)(j)!="") {
          //if (isInt(j)) { model.add(row, columnIris(j), vf.createLiteral(rows(i)(j).toInt)) }
          //else {  }
          model.add(row, columnIris(j), vf.createLiteral(rows(i)(j)))
        }
      }
    }

    val outFile="test.ttl"

    writeModel(outFile, model)

    //for { i <- 0 to rows.size } {
    //  val rowSubject=RDFUtils.iri(documentIRI.toString(), "row/" + i)
    //}


    //spark.stop()

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
