package com.knoldus.training

import java.io.{File, PrintWriter}

import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

object ConvertFormats {

  import java.io.InputStream
  def main(args: Array[String]): Unit = {

    //val filename = "test_manual_mod_2.ttl"
    //val outputFile = "test_manual_mod_2.ntrips"

    val filename = args(0)
    val outputFile = args(1)

    val input: InputStream = getClass.getResourceAsStream("/" + filename)

    // Rio also accepts a java.io.Reader as input for the parser.
    val model = Rio.parse(input, "", RDFFormat.TURTLE)

    val pw = new PrintWriter(new File(outputFile))
    Rio.write(model, pw, RDFFormat.NTRIPLES)
  }
}
