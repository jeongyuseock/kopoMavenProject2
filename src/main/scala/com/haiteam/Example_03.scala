package com.haiteam

import org.apache.spark.sql,SparkSession

object Example_03 {

  def main(args: Array[String]): Unit = {

    var staticUrl = "jdbc:oracle:thin:@192.168.111.28:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_ex"

    val selloutDataFromOracle= spark.read.format("jdbc").
    options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromOracle
  }
}
