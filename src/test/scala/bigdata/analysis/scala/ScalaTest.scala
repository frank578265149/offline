package bigdata.analysis.scala

import java.text.SimpleDateFormat

import bigdata.analysis.scala.utils.{ConfigurationManager, Getip}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import scala.tools.scalap.scalax.util.StringUtil
import scala.util.Random

/**
  * Created by frank on 17-3-2.
  */
class ScalaTest extends FunSuite with Matchers with SharedSparkContext {
  test("producing memberpoints test data") {

    val userSchema:String="serviceId|logTime|logid|eid|ecode|ename|customerId|customerCode|customerName|phone|activityId|" +
      "activityName|type|createTime|updateTime|openId|payOpenId|headIcon|nickName|birthYear|birthProvince|" +
      "birthCity|nowProvince|nowCity|sex|tyopenId|createIp"
    val hiveContext=new HiveContext(sc)
    val prizeFileRDD = hiveContext.sparkContext.wholeTextFiles("/home/frank/IdeaProjects/tracing-source/log/user", 3)
    val prizeRDD = prizeFileRDD.flatMap { tuple =>
      val fileContent = tuple._2
      fileContent.split("\n")
    }
    val userDF=convertDF(prizeRDD,userSchema,hiveContext)
    import hiveContext.implicits._
    userDF.filter(($"sex".isNotNull) and !($"sex".===("null") or ($"sex" === ""))).select(
      $"sex",$"openid").groupBy("openid").agg(first($"sex") as "first_sex").show()




  }
  /**
    *
    * @param rdd
    * @return
    */
  private def generateRDDRow(rdd:RDD[String],schemaString:String):RDD[Row]={
    val logSize:Int=schemaString.split("\\|",-1).size
    rdd.map{str=>
      val strSeq = str.split("\\|", -1)
      Row(strSeq:_*)
    }
  }

  /**
    *
    * @param rdd
    * @param schema
    * @param sqlContext
    * @return
    */
  private def registerDF (rdd: RDD[Row], schema: StructType, sqlContext: SQLContext): DataFrame = {
    sqlContext.createDataFrame(rdd, schema)
  }
  /**
    *
    * @param schemaString
    * @return
    */
  private def generateSchema(schemaString:String):StructType={
    val fields =schemaString.split("\\|",-1).map(filedName=> StructField(filedName,StringType,nullable = true))
    StructType(fields)
  }
  def  convertDF(rdd:RDD[String],schemaString:String,hiveContext:HiveContext):DataFrame={
    //Generate the schema based on  the string of schema
    val filterRDD=filterLog(rdd,schemaString)
    val schema=generateSchema(schemaString)
    val rowRDD=generateRDDRow(filterRDD,schemaString)
    val df=registerDF(rowRDD,schema,hiveContext)
    df
  }
  /**
    *
    * @param rdd
    * @param schema
    * @return
    */
  private def filterLog(rdd:RDD[String],schema:String):RDD[String]={
    val logSize:Int=schema.split("\\|",-1).size
    val result=rdd.filter(_.split("\\|",-1).size ==logSize)


    result
  }
}
