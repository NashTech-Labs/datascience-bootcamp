name := "Training"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test

libraryDependencies += "com.typesafe" % "config" % "1.3.2"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-util" % "2.5.0"
libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-model" % "2.5.0"
libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-query" % "2.5.0"
libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-rio-api" % "2.5.0"
libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-rio-turtle" % "2.5.0"
libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-repository-api" % "2.5.0"
libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-runtime" % "2.5.0"
libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-rio-rdfxml" % "2.5.0"
libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-repository-sail" % "2.5.0"
libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-sail-memory" % "2.5.0"

val ryaVersion = "3.2.12-incubating"

// https://mvnrepository.com/artifact/org.apache.rya/rya.api
libraryDependencies += "org.apache.rya" % "rya.api" % ryaVersion

// https://mvnrepository.com/artifact/org.apache.rya/accumulo.rya
libraryDependencies += "org.apache.rya" % "accumulo.rya" % ryaVersion

// https://mvnrepository.com/artifact/org.apache.rya/rya.sail
libraryDependencies += "org.apache.rya" % "rya.sail" % ryaVersion



