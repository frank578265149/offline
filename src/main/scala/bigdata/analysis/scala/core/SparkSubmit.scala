package bigdata.analysis.scala.core
import java.io._
import java.text.SimpleDateFormat

import bigdata.analysis.scala.dao.jdbc.{AccountsSum, ActivityCount, CountPrizeSum, _}
import bigdata.analysis.scala.service.JDBCService
import bigdata.analysis.scala.service.impl.JDBCServiceImpl
import bigdata.analysis.scala.utils.{DateUtils, StringUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._

import scala.util.Properties


/**
  * Created by frank on 17-3-21.
  */
/**
  * Whether to submit, kill, or request the status of an application.
  * The latter two operations are currently supported only for standalone and Mesos cluster modes.
  */
object SparkSubmitAction extends Enumeration {

  type SparkSubmitAction = Value
  val SUBMIT, KILL, REQUEST_STATUS = Value
}
object SparkSubmit  extends  CommandLineUtils{
  // Cluster managers
  private val mysqlService:JDBCService=new JDBCServiceImpl()
  private val YARN = 1
  private val STANDALONE = 2
  private val MESOS = 4
  private val LOCAL = 8
  private val ALL_CLUSTER_MGRS = YARN | STANDALONE | MESOS | LOCAL

  // Deploy modes
  private val CLIENT = 1
  private val CLUSTER = 2
  private val ALL_DEPLOY_MODES = CLIENT | CLUSTER

  // Special primary resource names that represent shells rather than application jars.
  private val SPARK_SHELL = "spark-shell"
  private val PYSPARK_SHELL = "pyspark-shell"
  private val SPARKR_SHELL = "sparkr-shell"
  private val SPARKR_PACKAGE_ARCHIVE = "sparkr.zip"
  private val R_PACKAGE_ARCHIVE = "rpkg.zip"

  private val CLASS_NOT_FOUND_EXIT_STATUS = 101
  // scalastyle:off println
  def printVersionAndExit(): Unit = {
    printStream.println("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
                        """.format("1.6"))
    printStream.println("Using Scala %s, %s, %s".format(
      Properties.versionString, Properties.javaVmName, Properties.javaVersion))
    printStream.println("Type --help for more information.")
    exitFn(0)
  }
  // scalastyle:on println
  override def main(args: Array[String]): Unit = {
    val appArgs=new SparkSubmitArguments(args)
    if(appArgs.verbose){
      printStream.println(appArgs)
    }
    appArgs.action match  {
      case SparkSubmitAction.SUBMIT => submit(appArgs)
    }

  }
  private def submit(args:SparkSubmitArguments):Unit={
    val conf=new SparkConf()
    conf.setAppName("offline")
    conf.setMaster("local")
    val sc=new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId",args.sparkProperties.getOrElse("spark.offline.aws.key",""))
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",args.sparkProperties.getOrElse("spark.offline.aws.secret",""))
    val sqlContext=new SQLContext(sc)
    val prizeSchema=args.sparkProperties.get("spark.offline.prize.schema").get
    val prizeDir=args.sparkProperties.get("spark.offline.prize.dir").get
    val prizeRawRDD=loadRDD(prizeDir,sqlContext,3)
    val prizeDF=convertDF(prizeRawRDD,prizeSchema,sqlContext)
    dobusiness(args,sqlContext,prizeDF)

  }
  private def dobusiness(args:SparkSubmitArguments,hiveContext:SQLContext,df:DataFrame):Int={
    import hiveContext.implicits._

    val drawPrizeSum=df.select($"isStatus",$"isPrize",$"activityId",$"productId",(when( ($"createTime" equalTo "null" )or ($"createTime" equalTo ""),lit("2016-02-12 12:12:12" )).otherwise($"createTime")) as ("time"),$"ecode")
      .select(
        getDrawTableId(date_format($"time","yyyyMMdd"),$"activityId",$"productId").as("id"),
        $"ecode",
        date_format($"time","yyyy-MM-dd") as ("date"),
        $"activityId",$"productId",
        $"isStatus",
        $"isPrize"
      ).groupBy($"id",$"ecode",$"date",$"activityId",$"productId")
      .agg(
        {sum(when($"isStatus" === 0 ,1).otherwise(0)) alias("prizeDrawCount")},
        {sum(when($"isStatus" === 0 and $"isPrize" === 0 ,1).otherwise(0)) alias("notwinningCount")}
      )
    //  drawPrizeSum.show
    drawPrizeSum.foreachPartition{it=>
      while(it.hasNext){
        val row=it.next()
        logger.info(s"write drawPrizeSum to mysql >>>>>>>>")
        saveOrUpdateDrawPrizeSum(DrawPrizeSum(id=row.getString(0),ecode=row.getString(1),
          date=row.getString(2),activity = row.getString(3),
          product = row.getString(4),prizeDrawCount = row.getLong(5),
          notWinningCount = row.getLong(6)),mysqlService)
      }
    }
    //duijiang
    val convertPrizeSum1=df.select($"activityId",$"productId",$"prizeId",$"isStatus",$"prizeId",$"ecode",
      (when( ($"getTime" equalTo "null" )or ($"getTime" equalTo ""),lit("2016-02-12 12:12:12" )).otherwise($"getTime")) as ("time"),$"prizeAmount")
      .select(getConvertId(date_format($"time","yyyyMMdd"),$"activityId",$"productId",$"prizeId") as ("id"),
        $"ecode",
        date_format($"time","yyyy-MM-dd") as ("date"),
        $"activityId",$"productId",$"prizeId",
        $"isStatus",
        $"prizeAmount"
      ).groupBy(
      $"id",
      $"ecode",
      $"date",
      $"activityId",
      $"productId",
      $"prizeId"
    ).agg(
      {sum(when($"isStatus" === 2 ,1).otherwise(0)) alias("count_pat_mode")},
      {sum(when($"isStatus" === 2 ,$"prizeAmount").otherwise(0)) alias("sum_prize_amount")}
    )
    convertPrizeSum1.foreachPartition{it=>
      while(it.hasNext){
        val row=it.next()
        logger.info(s"write convertPrizeSum(duijiang) to mysql >>>>>>")
        saveOrUpdateConvertPrizeSum(ConvertPrizeSum(id=row.apply(0).toString,ecode=row.apply(1).toString,
          date=row.apply(2).toString,activity=row.getString(3),product=row.apply(4).toString,
          prize=row.getString(5), countPatMode=row.getLong(6),
          sumPrizeAmount=row.getLong(7).toDouble/100,countDrawnMode = 0,sumPossessAmount = 0.0),mysqlService)
      }
    }
    //zhongjiang
    val convertPrizeSum2=df.select($"activityId",$"productId",$"prizeId",$"isStatus",$"prizeId",$"ecode"
      ,(when( ($"acceptTime" equalTo "null" )or ($"acceptTime" equalTo ""),lit("2016-02-12 12:12:12" )).otherwise($"acceptTime")) as ("time"),$"prizeAmount")
      .select(getConvertId(date_format($"time","yyyyMMdd"),$"activityId",$"productId",$"prizeId") as ("id"),
        $"ecode",
        date_format($"time","yyyy-MM-dd") as ("date"),
        $"activityId",$"productId",$"prizeId",
        $"isStatus",
        $"prizeAmount"
      ).groupBy($"id",$"ecode",$"date",$"activityId",$"productId",$"prizeId")
      .agg(
        {sum(when($"isStatus" === 1 ,1).otherwise(0)) alias("count_draw_mode")},
        {sum(when($"isStatus" === 1 ,$"prizeAmount").otherwise(0)) alias("sum_possess_amount")}
      )


    convertPrizeSum2.foreachPartition{it=>
      while(it.hasNext){
        val row=it.next()
        logger.info(s"write convertPrizeSum(zhongjiang) to mysql >>>>")
        saveOrUpdateConvertPrizeSum(ConvertPrizeSum(id=row.getString(0),ecode=row.getString(1), date=row.getString(2),
          activity=row.getString(3),product=row.getString(4), prize=row.getString(5), countPatMode=0, sumPrizeAmount=0.0,
          countDrawnMode =row.getLong(6) ,sumPossessAmount = row.getLong(7).toDouble/100),mysqlService)
      }
    }

    val shopPrizeSum=df.select($"activityId",$"productId",$"prizeId",(when( ($"getTime" equalTo "null") or ($"getTime" equalTo ""),lit("2016-02-12 12:12:12" )).otherwise($"getTime")) as ("time"),$"isStatus",$"shopId",$"ecode")
      .select(getShopTableId(date_format($"time","yyyyMMdd"),$"activityId",$"productId",$"prizeId",$"shopId") as ("id"),
        $"ecode",
        date_format($"time","yyyy-MM-dd") as ("date"),
        $"activityId",$"productId",$"prizeId",
        $"shopId",
        $"isStatus"
      ).groupBy($"id",$"ecode",$"date",$"activityId",$"productId",$"prizeId",$"shopId")
      .agg(
        {sum(when($"isStatus" === 2 and $"shopId".>(0) ,1).otherwise(0)) alias("count_pat_mode")}
      )
    shopPrizeSum.foreachPartition{it=>
      while(it.hasNext){
        val row=it.next()
        if(!StringUtils.isEmpty(row.getString(6))){
          saveOrUpdateShopPrizeSum(ShopPrizeSum(id=row.getString(0).toString,ecode=row.getString(1),date=row.getString(2),
            activity = row.getString(3),product = row.getString(4),prize = row.getString(5),shop=row.getString(6),row.getLong(7)),mysqlService)
        }

      }
    }
    //经销商

    val dealerPrizeSum=df.select($"activityId",$"productId",$"prizeId",$"prizeType",$"prizeAmount",(when( ($"getTime" equalTo "null") or ($"getTime" equalTo ""),lit("2016-02-12 12:12:12" )).otherwise($"getTime")) as ("time"),$"isStatus",$"dealerId",$"ecode")
      .select(getDealerTableId(date_format($"time","yyyyMMdd"),$"activityId",$"productId",$"prizeId",$"dealerId") as ("id"),
        $"ecode",
        date_format($"time","yyyy-MM-dd") as ("date"),
        $"activityId",$"productId",$"prizeId",
        $"dealerId",$"prizeAmount",
        $"prizeType",$"isStatus"

      ).groupBy($"id",$"ecode",$"date",$"activityId",$"productId",$"prizeId",$"dealerId")
      .agg(
        {sum(when($"isStatus" === 2 and $"dealerId".>(0) ,1).otherwise(0)) alias("count_pat_mode")},
        {sum(when($"isStatus" === 2  and ($"prizeType" ===2  or $"prizeType" === 6 ),$"prizeAmount").otherwise(0)) alias("sum_money")}
      )
    dealerPrizeSum.foreachPartition{it=>
      while(it.hasNext){
        val row=it.next()
        if(!StringUtils.isEmpty(row.getString(6))){
          saveOrUpdateDealerPrizeSum(DealerPrizeSum(id=row.getString(0).toString,ecode=row.getString(1),date=row.getString(2),
            activity = row.getString(3),product = row.getString(4),prize = row.getString(5),dealer=row.getString(6),countPatMode=row.getLong(7),sumPrizeAmount=row.getLong(8).toDouble/100),mysqlService)
        }

      }
    }
    //对账

    val accountSum=df.select(
      (when( ($"getTime" equalTo "null") or ($"getTime" equalTo ""),lit("2016-02-12 12:12:12" )).otherwise($"getTime") )as ("time"),$"activityId",$"productId",$"ecode",$"isStatus",$"prizeType",$"prizeAmount")
      .select(getAccountSumId(date_format($"time","yyyyMMdd"),$"activityId",$"productId") as ("id"),
        $"isStatus",
        $"prizeType",
        $"ecode",
        date_format($"time","yyyy-MM-dd") as ("date"),
        $"activityId",
        $"productId",
        $"prizeAmount").
      groupBy($"id",$"ecode",$"date",$"activityId",$"productId")
      .agg(
        {sum(when($"isStatus" === 2  and ($"prizeType" ===2  or $"prizeType" === 6 ),$"prizeAmount").otherwise(0)) alias("sum_money")},
        {sum(when($"isStatus" === 2  and ($"prizeType" ===2  or $"prizeType" === 6 ),when($"prizeAmount".>=(100),$"prizeAmount").otherwise(0)).otherwise(0)) alias("tidyMoney")},
        {sum(when($"isStatus" === 2  and ($"prizeType" ===2  or $"prizeType" === 6 ),when($"prizeAmount".<(100),$"prizeAmount").otherwise(0)).otherwise(0)) alias("changeMoney")}

      )
    accountSum.foreachPartition{it=>
      while(it.hasNext){
        val row=it.next()
        logger.info(s"write AccountSum to mysql >>>>>>>>>>>>")
        saveOrUpdateAccountSum(AccountsSum(id=row.getString(0),ecode=row.getString(1),date=row.getString(2),
          activity_id=row.getString(3),product_id=row.getString(4), sum_money=row.getLong(5).toDouble/100,
          tidy_money=row.getLong(6).toDouble/100,change_money=row.getLong(7).toDouble/100),mysqlService)
      }
    }
    1
  }
  def generateFileName(appArgs:SparkSubmitArguments,env:Map[String,String]=sys.env):String={

     appArgs.sparkProperties.get("spark.offline.data.dir.prefix")
       .orElse(Option(env.get("spark.offline.data.dir.prefix")))
       .map{ t=> s"$t${File.separator}${DateUtils.getYesterdayDateShort}"}
       .orNull

  }
  /**
    * Return whether the given primary resource represents a shell.
    */
  def isShell(res: String): Boolean = {
    (res == SPARK_SHELL || res == PYSPARK_SHELL || res == SPARKR_SHELL)
  }
  /**
    * Return whether the given main class represents a sql shell.
    */
  def isSqlShell(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver"
  }
  /**
    * Return whether the given primary resource requires running python.
    */
  def isPython(res: String): Boolean = {
    res != null && res.endsWith(".py") || res == PYSPARK_SHELL
  }

  /**
    * Return whether the given primary resource requires running R.
    */
  def isR(res: String): Boolean = {
    res != null && res.endsWith(".R") || res == SPARKR_SHELL
  }

  def isInternal(res: String): Boolean = {
    //res == SparkLauncher.NO_RESOURCE
    false
  }
  /**
    * Return whether the given primary resource represents a user jar.
    */
  def isUserJar(res: String): Boolean = {
    !isShell(res) && !isPython(res) && !isInternal(res) && !isR(res)
  }
  def loadRDD(path:String,sqlContext: SQLContext,par:Int):RDD[String]={
    val prizeFileRDD = sqlContext.sparkContext.wholeTextFiles(path,par)
    val prizeRDD=prizeFileRDD.flatMap{tuple=>
      val fileContent=tuple._2
      fileContent.split("\n")
    }
    prizeRDD
  }
  def  convertDF(rdd:RDD[String],schemaString:String,hiveContext:SQLContext):DataFrame={
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
    * @param schemaString
    * @return
    */
  private def generateSchema(schemaString:String):StructType={
    val fields =schemaString.split("\\|",-1).map(filedName=> StructField(filedName,StringType,nullable = true))
    StructType(fields)
  }
  /**
    *
    * @param rdd
    * @param schema
    * @param sqlContext
    * @return
    */
  private def registerDF(rdd:RDD[Row],schema:StructType,sqlContext:SQLContext):DataFrame={
    sqlContext.createDataFrame(rdd,schema)
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
  val getDrawTableId=udf{( createTime:String,activityId:String,productId:String)=>{

    val id=s"${createTime}${activityId}${productId}"
    id


  }}
  val getConvertId=udf{( time:String,activityId:String,productId:String,prizeId:String)=>{

    val id=s"${time}${activityId}${productId}${prizeId}"
    id
  }}
  val getShopTableId=udf{( getTime:String,activityId:String,productId:String,prizeId:String,shopId:String)=>{
    val id=s"${getTime}${activityId}${productId}${prizeId}${shopId}"
    id
  }}
  val getDealerTableId=udf{( getTime:String,activityId:String,productId:String,prizeId:String,dealerId:String)=>{
    val id=s"${getTime}${activityId}${productId}${prizeId}${dealerId}"
    id
  }}
  val getAccountSumId=udf{( getTime:String,activityId:String,productId:String)=>{

    val id=s"${getTime}${activityId}${productId}"
    id


  }}
  /**
    *
    * @param drawPrizeSum
    * @param mysqlService
    * @return
    */
  private def saveOrUpdateDrawPrizeSum(drawPrizeSum: DrawPrizeSum,mysqlService:JDBCService):Int={

    mysqlService.saveOrUpdateDrawPrizeSum(drawPrizeSum)
  }
  // duijiang and zhongjiang
  private def saveOrUpdateConvertPrizeSum(convertPrizeSum: ConvertPrizeSum,mysqlService:JDBCService):Int={
    mysqlService.saveOrUpdateConvertPrizeSum(convertPrizeSum)
  }
  /**
    *
    * @param shopPrizeSum
    * @param mysqlService
    * @return
    */
  private def saveOrUpdateShopPrizeSum(shopPrizeSum: ShopPrizeSum,mysqlService:JDBCService):Int={
    mysqlService.saveOrUpdateShopPrizeSum(shopPrizeSum)
  }

  /**
    *
    * @param dealerPrizeSum
    * @param mysqlService
    * @return
    */
  private def saveOrUpdateDealerPrizeSum(dealerPrizeSum: DealerPrizeSum,mysqlService:JDBCService):Int={
    mysqlService.saveOrUpdateDealerPrizeSum(dealerPrizeSum)
  }

  /**
    *
    * @param accountsSum
    * @param mysqlService
    * @return
    */
  private def saveOrUpdateAccountSum(accountsSum: AccountsSum,mysqlService:JDBCService):Int={
    mysqlService.saveOrUpdateAccountSum(accountsSum)
  }

  /**
    *
    * @param countPrizeSum
    * @param mysqlService
    * @return
    */
  private def saveOrUpdateCountPrizeSum(countPrizeSum: CountPrizeSum,mysqlService:JDBCService):Int={
    mysqlService.saveOrUpdateCountPrizeSum(countPrizeSum)
  }


  /**
    *
    * @param activityCount
    * @param mysqlService
    * @return
    */
  private def saveOrUpdateStatisticsSum(activityCount: ActivityCount,mysqlService:JDBCService):Int={
    mysqlService.saveOrUpdateStatisticsSum(activityCount)

  }

}
