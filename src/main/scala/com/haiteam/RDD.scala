package com.haiteam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType} //RDD에서 DataFram으로 변환할대 임폴트해줘야함
import org.apache.spark.sql.Row
object RDD {

  def main(args: Array[String]): Unit = {

    // 스파크에서 오라클 불러와서 레프트 조인하는법

    val spark = SparkSession.builder().appName("mavenProject").
      config("spark.master", "local").
      getOrCreate()


    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var masterDb = "kopo_product_mst"

    val selloutDf = spark.read.format("jdbc").option("encoding", "UTF-8").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    val productMasterDF = spark.read.format("jdbc").option("encoding", "UTF-8").
      options(Map("url" -> staticUrl, "dbtable" -> masterDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDf.createOrReplaceTempView("selloutTable") //임시 테이블생성
    productMasterDF.createOrReplaceTempView("mstTable") //  ( ) 안이 임시테이블이름

    var rawData = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as KEYCOL," + // concat   (  ) 안에 있는것들을 키콜로 묶는다
      "a.regionid AS ACCOUNTID, " + // as  ###   -> ### 으로 저장한다
      "a.product AS PRODUCT, " +
      "a.yearweek AS YEARWEEK, " +
      "cast(qty as double) AS QTY, " +
      "b.product_name AS PRODUCTNAME " +
      "from selloutTable a " + //임시테이블 a 넣고
      "left join mstTable b " + //임시테이블 b 넣으면
      "on a.product = b.product_id") //   on = 키를 넣겠다 a.pro와 b.pro_id가 같을때 실행해라


    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("KEYCOL")
    var accountidNo = rawDataColumns.indexOf("ACCOUNTID")
    var productNo = rawDataColumns.indexOf("PRODUCT")
    var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
    var qtyNo = rawDataColumns.indexOf("QTY")
    var productnameNo = rawDataColumns.indexOf("PRODUCTNAME") //스파크에서 대소문자 구분함ㅇㅋ?


    // 데이터형 변환 [데이터프레임 → RDD]
    //var {RDD변수명} = {데이터프레임 변수명}.rdd

    var rawRdd = rawData.rdd //HHH

    //( KEYCOL, ACCOUNTID, PRODUCT, YEARWEEK, QTY, PRODUCTNAME)
    var rawExRdd = rawRdd.filter(x => {
      var checkValid = true

      //설정 부적합 로직
      if (x.getString(3).length != 6) {
        var checkValid = false
      }
      checkValid

    })
    //A60 PRODUCT34 201402 4463
    var rawExrdd = rawRdd.filter(x => {
      var checkValid = false
      if ((x.getString(accountidNo) == "A60") &&
        (x.getString(productnameNo) == "PRODUCT34") &&
        (x.getString(yearweekNo) == "201402")) {
        checkValid = true
      }
      checkValid


    })


    //실습:자신이 생성한 RDD에  연주차 정보가 52보다 큰값을 제거하는 로직구현
    //A60 PRODUCT34 201402 4463
    var rawExrdd2 = rawRdd.filter(x => {
      // boolean = true
      var checkValid = true
      // 찾기 : yearweek 인덱스로 주자정보만 인트타입으로 변환
      var weekValue = x.getString(yearweekNo).substring(4).toInt
      // 비교한후 주차정보가 53 이상인 경우 레코드 삭제
      if (weekValue >= 53) {
        checkValid = false
      }
      checkValid
    })



    //실습 : 상품정보가 PRODUCT1,2, 인 정보만 필터링 하세요

    // 분석대상 제품군 등록
    var productArray = Array("PRODUCT1", "PRODUCT2")

    // 세트 타입으로 변환
    var productSet = productArray.toSet

    var resultRdd = rawRdd.filter(x => {           //이건 rdd.filter의 정의
      var checkValid = false

      // 데이터 특징 행의 product 컬럼인덱스를 활용하여 데이터 대입
      var productInfo = x.getString(productNo);

      if (productSet.contains(productInfo)) {
        checkValid = true
      }
      checkValid //이걸 주석하고 밑에걸 넣어도도
    })
    // 2번째 방법
    // if((productInfo == "PRODUCT1") ||        //productArray(0)
    //    (productInfo == "PRODUCT2")) {        //productArray(1)
    //    checkValid = true



    //화면에 찍는법 3줄   resultRdd.take(3)foreach(println)
    //             전체  resultRdd.collect.toArray.foreach(println)



    // 데이터형 변환 [RDD → Dataframe]
//    var {DataFrame 변수명} = spark.createDataframe( {RDD명},
//      StructType(Seq( StructField( “컬럼명#1”, 데이터타입#1),
//                      StructField( “컬럼명#2”, 데이터타입#2))


    val DataFrameRdd = spark.createDataFrame(resultRdd,
      StructType(
        Seq(
          StructField("KEYCOL", StringType),              // 내가 위에 있던 컬럼명을 넣어야하고
          StructField("ACCOUNTID", StringType),
          StructField("PRODUCT", StringType),
          StructField("YEARWEEK", StringType),
          StructField("QTY", DoubleType),               //QTY는  Double이기때문에 StringType이 아닌 DoubleType 이라고 써야함.
          StructField("PRODUCTNAME", StringType))))

    var mapRdd = rawRdd.map(x=>{
      var qty = x.getDouble(qtyNo).toDouble       // qty가 double이라서  원래 getString인데 getDouble 이라고 바꿔야함
      var maxValue = 700000
      if(qty > 700000){qty = 700000}
      Row( x.getString(keyNo),
        x.getString(yearweekNo),
        qty)//x.getString(qtyNo)
    })
    // qty가 double이라서 getString아니라 getDouble

    //  mapRdd.take(3)foreach(println)           Rdd는 show가    안됨 이걸로 찍어서 봐야함
    //             전체찍는거 resultRdd.collect.toArray.foreach(println)

    //mapRdd.first
    // mapRdd.first= ( 키정보 , 지역정보 ,상품정보 ,

    //처리로직 : 거래량 maxvalue 이상인것은 maxvalue로 치환한다.
    var MAXVALUE = 70000
    var mapRdd2 = rawRdd.map(x=>{
      //디버킹코드: var x = mapRdd.filter(x=>{ x.getDouble(qtyNo) > 70000 }).first
      //로직후현예정
      var org_qty = x.getDouble(qtyNo)
      var new_qty = org_qty

      if (new_qty > MAXVALUE){
        new_qty = MAXVALUE
      }
     // import org.apache.spark.sql.Row   위에서 row를 임포트해줘야 쓸수있음.스파크에서도 해야함
     // row를 안쓰면 인덱스로 들어가서 컬럼값을 들어온 순서대로 써야함 row
      //출력 row 키정보, 연주차정보 ,거래량 정보_org, 거래량 정보_new )

      Row( x.getString(keyNo),
        x.getString(yearweekNo),
        org_qty,
        new_qty)
    })



    // 디버깅   :    그냥 로직이 어떻게되어있나 보여주는정도임
    var mapexRdd2 = rawRdd
    var x = mapexRdd2.first


  }
}
