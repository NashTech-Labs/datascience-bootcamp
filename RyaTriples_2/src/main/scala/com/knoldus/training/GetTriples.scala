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

import org.eclipse.rdf4j.model.{Model, Statement, ValueFactory}
import org.eclipse.rdf4j.model.impl.{SimpleValueFactory, TreeModel}
import org.eclipse.rdf4j.model.util.ModelBuilder
import org.eclipse.rdf4j.model.vocabulary.{DC, DCAT, FOAF, OWL, RDF, SKOS}
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
    val rows=sc.textFile("/home/jouko/dev/project/TrainingSprints/TrainingSprint8/RyaTriples/src/main/resources/Legacy_Purchase_Orders.csv")
      .map( x => x.split(",") )
    rows.collect.foreach(println)


    //spark.stop()

  }


}
