package com.haiteam

import org.apache.spark.sql.SparkSession;



object Example_Join {



  //spark연동
  val spark = SparkSession.builder().appName("hkProject").
    config("spark.master", "local").
    getOrCreate()

  // dataPath :경로설정   메인,서브 각각 설정
  var dataPath = "c:/spark/bin/data/"
  var mainData = "kopo_channel_seasonality_ex.csv"
  var subData = "kopo_product_mst.csv"

  //DataFrame
  var mainData1 = spark.read.format("csv").
    option("header", "true").load(dataPath + mainData)
  //DataFrame
  var subData1 = spark.read.format("csv").
    option("header", "true").load(dataPath + subData)


  // (임시)테이블생성 일단가볍게 쓰는용도? 이리저리
  mainData1.createOrReplaceTempView("maindata")
  subData1.createOrReplaceTempView("subdata")

  var test3 = spark.sql("select a.productgroup, b.productname " +
    "from maindata a " +
    "left join subdata b " +
    "on a.productgroup = b.productid")


  var leftJoinData =

  //a.productgroup b.productid
  var test2 = spark.sql("select a.regionid, a.productgroup, b.productname, a.yearweek, a.qty " +
    "from maindata a " + " left join subdata b " +
    "on a.productgroup = b.productid")



  // 오라클 연결하기
  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "kopo_channel_seasonality_new"
  var selloutDb2 = "kopo_region_mst"


  //데이터 읽어오기
  val selloutData= spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load
  //테이블생성
  selloutData.createOrReplaceTempView("selloutTable")

  // 데이터 읽어오기
  val selloutData2= spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> selloutDb2, "user" -> staticUser, "password" -> staticPw)).load
  //테이블생성
  selloutData2.createOrReplaceTempView("selloutTable2")    //(sellouttable2) 가 생성한 테이블이름

  selloutData.show()



  spark.sql("select a.product, b.regionname " +
    "from selloutTable a " +
    "left join selloutTable2 b " +
    "on a.regionid = b.regionid")

  spark.sql("select a.regionid, b.regionname " +
    "from selloutTable a " +
    "inner join selloutTable2 b " +
    "on a.regionid = b.regionid")
















}