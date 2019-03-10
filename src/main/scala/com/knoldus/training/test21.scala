package com.knoldus.training

import com.knoldus.common.AppConfig
import org.apache.log4j.Logger
object test21 {


  def main(args: Array[String]) = {

    val LOGGER: Logger = SSHLogger.getLogger(Reports.getClass)
    val age = 20

    val a = calculate401k(age) _

    println(AppConfig.port)

    println(a(monkey401Kprocess))
    println(a(human401))


  }

  def calculate401k(age: Int) ( func: (Int) => String): String = {

    return func(age)

  }





  def monkey401Kprocess(i: Int) = {

    i.toString + "I am a monkey"
  }

  def human401(i :Int) = {
    i + " I am a man"
  }
}
