package com.knoldus.common

import com.typesafe.config.Config

object AppConfig {
  import com.typesafe.config.ConfigFactory

  val conf :Config = ConfigFactory.load()
  val port = conf.getInt("application.cassandra.port")

}
