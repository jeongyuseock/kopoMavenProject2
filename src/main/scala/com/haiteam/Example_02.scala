package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_02 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    ///////////////////////////     Postgres / GreenplumDB 데이터 로딩 ////////////////////////////////////
    // 접속정보 설정
//    var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
//    var staticUser = "kopo"
//    var staticPw = "kopo"
//    var selloutDb = "kopo_channel_seasonality"
//
//    // jdbc (java database connectivity) 연결
//    val selloutDataFromPg= spark.read.format("jdbc").
//      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load
//
//    // 메모리 테이블 생성
//    selloutDataFromPg.createOrReplaceTempView("selloutTable")
//
//
//
//    // 파일설정// 파일설정
//    var staticUrl1 = "jdbc:mysql://192.168.110.112:3306/kopo"
//    var staticUser1 = "root"
//    var staticPw1 = "P@ssw0rd"
//    var selloutDb1 = "KOPO_PRODUCT_VOLUME"
//
//    // jdbc (java database connectivity)
//    val selloutDataFromMysql= spark.read.format("jdbc").
//      options(Map("url" -> staticUrl1,"dbtable" -> selloutDb1,"user" -> staticUser1, "password" -> staticPw1)).load
//
//    selloutDataFromMysql.createOrReplaceTempView("selloutTable")

    // 파일설정
//    var staticUrl3 = "jdbc:sqlserver://192.168.110.70;databaseName=kopo"
//    var staticUser3 = "haiteam"
//    var staticPw3 = "haiteam"
//    var selloutDb3 = "KOPO_PRODUCT_VOLUME"
//
//    // jdbc (java database connectivity) 연결
//    val selloutDataFromSqlserver= spark.read.format("jdbc").
//      options(Map("url" -> staticUrl3,"dbtable" -> selloutDb3,"user" -> staticUser3, "password" -> staticPw3)).load
//
//    selloutDataFromSqlserver.createOrReplaceTempView("selloutTable")
//
//
//    var score = 9
//    var level = "기타"
//    if ( score > 9){
//      level = "수"
//    }else if ( score >8)
//      ( score >9)){
//      level


    } // 디버깅
    var priceData = Array(100.0,1200.0,1400.0)
    var promotionRate = 0.2
    var priceDataSize = priceData.size
    var i=0
             while (i<priceDataSize){
               var promotionEffect = priceData(i) * promotionRate
               priceData(i) = priceData(i) - promotionEffect
               i=i+1
               println(priceData(i))
             }
  var priceData2 = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
  var promotionRate2 = 0.2
  var priceDataSize2 = priceData.size

  for(i <-0 until priceDataSize){
    var promotionEffect = priceData(i) * promotionRate
    priceData(i) = priceData(i) - promotionEffect

    priceData.map(x=>{ x-(x*promotionRate)})   // 맵함수를쓴거 위에 포문과 같은결과가나옴
  }



  var productData = ("pen","boat","cak")
  var productName = ("kopo_jys")
  var productDataSize = priceData.size

  for (i <-0 until productDataSize){
    productData(i) = productName + productData(i)
  }

def discountedPrice(price:Double, rate:Double) : Double = {
  var discount = price * rate
  var returnValue = price - discount
  returnValue
}
  var orgRate = 0.2
  var orgPrice = 2000
  var newPrice =
    discountedPrice(orgPrice,orgRate)

  var a = 15.125222
  var b = 15.147218
  var c = 69.72756

  //var ra = math.round(a*100)/100.0
  // var rb = math.round(b*100)100.0


//  prectice@@@  var a = 15.125222
//        var b = 15.147218
//  var c = 69.72756
//  값을 활용하여 1) 반올림 소수점 2자리
//    이후 합이 100이되도록 구현하세요
//  (* a 값에 나머지값은 추가하면됩니다

    def hkRound(targetValue:Double, sequence:Int)  : Double = {
      var multiValue = math.pow(10,sequence)
      var returnValue = math.round(targetValue*multiValue)/multiValue
      returnValue


    }



}