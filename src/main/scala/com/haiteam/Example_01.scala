package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    var testArray = Array(22,33,50,70,90,100)
        println(testArray)
    var anwer = testArray.filter (x=>{x%10 == 0}) //끝자리수가 0인걸 남기시오
    // ? println(anwer)

     //다른 방법으로 구하는법
    //var anwer = testArray.filter(x=>{10==0})
    //var data = x.toString
    //var datsSize = data.size




    }


  }

