package com.haiteam
import org.apache.spark.sql.SparkSession

object Spark_Oracle_Reft_Join {
  object Example_Join {
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

      selloutDf.createOrReplaceTempView("selloutTable")      //임시 테이블생성
      productMasterDF.createOrReplaceTempView("mstTable")     //  ( ) 안이 임시테이블이름

      var rawData = spark.sql("select " +
        "concat(a.regionid,'_',a.product) as KEYCOL,"+     // concat   (  ) 안에 있는것들을 키콜로 묶는다
        "a.regionid AS ACCOUNTID, " +                    // as  ###   -> ### 으로 저장한다
        "a.product AS PRODUCT, " +
        "a.yearweek AS YEARWEEK, " +
        "cast(qty as double) AS QTY, " +
        "b.product_name AS PRODUCTNAME " +
        "from selloutTable a " +          //임시테이블 a 넣고
        "left join mstTable b " +         //임시테이블 b 넣으면
        "on a.product = b.product_id")    //   on = 키를 넣겠다 a.pro와 b.pro_id가 같을때 실행해라


      var rawDataColumns = rawData.columns
      var keyNo = rawDataColumns.indexOf("KEYCOL")
      var accountidNo = rawDataColumns.indexOf("ACCOUNTID")
      var productNo = rawDataColumns.indexOf("PRODUCT")
      var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
      var qtyNo = rawDataColumns.indexOf("QTY")
      var productnameNo = rawDataColumns.indexOf("PRODUCTNAME")     //스파크에서 대소문자 구분함ㅇㅋ?

      var rawRdd = rawData.rdd   //HHH

      //( KEYCOL, ACCOUNTID, PRODUCT, YEARWEEK, QTY, PRODUCTNAME)
      var rawExRdd = rawRdd.filter(x=> {
        var checkValid = true

        //설정 부적합 로직
        if(x.getString(3).length != 6){
          var checkValid = false
        }
         checkValid

      })
        //A60 PRODUCT34 201402 4463
      var rawExrdd = rawRdd.filter(x=>{
        var checkValid = false
        if ((x.getString(accountidNo) == "A60") &&
          (x.getString(productnameNo) == "PRODUCT34") &&
          (x.getString(yearweekNo) == "201402")) {
          checkValid = true
        }
        checkValid


      })


                //실습  연주차 정보가 52보다 큰값을 제거하는 로직구현
                //A60 PRODUCT34 201402 4463
                var rawExrdd2 = rawRdd.filter(x=>{
                  var checkValid = true
                  if ((x.getString(yearweekNo) ==  ))
                     {
                    checkValid = false
                  }
                  checkValid


                })

    }






  }


}
