name := "Training"

version := "0.1"

scalaVersion := "2.11.1"

libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test

libraryDependencies += "com.typesafe" % "config" % "1.3.2"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0" % "provided"