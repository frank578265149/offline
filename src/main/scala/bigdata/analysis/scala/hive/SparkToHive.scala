package bigdata.analysis.scala.hive

import java.text.SimpleDateFormat

import bigdata.analysis.scala.dao.{MultiClientByte, KafkaToSpark}
import bigdata.analysis.scala.dao.hbase.HbaseConf
import bigdata.analysis.scala.dao.jdbc._
import bigdata.analysis.scala.hive.SparkToHive.MainConfig
import bigdata.analysis.scala.service.JDBCService
import bigdata.analysis.scala.service.impl.JDBCServiceImpl
import bigdata.analysis.scala.utils.{ConfigurationManager, StringUtils}
import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
/**
  * Created by frank on 16-10-13.
  */
class SparkToHive(@transient val sparkContext:SparkContext, @transient hiveContext:HiveContext,wfc:MainConfig)  extends  KafkaToSpark  with Serializable{

  //<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
  val schemaString1="serviceId|logTime|logid|eid|ecode|ename|prizeOrderId|barcode|activityId|activityName|prizeAmount|" +
    "prizeIntegration|productId|productName|prizeType|prizeTypeName|isType|isTypeName|userId|userName|openId|isStatus|" +
    "isStatusName|isPrize|isPrizeName|prizeId|prizeName|createTime|acceptTime|getTime|isFirstGet|isFirstGetName|payOpenId|" +
    "redBagType|redBagTypeName|isActive|isActiveName|dealerId|dealerName|shopId|shopName|tyopenId"

  val schemaString2="serviceId|logTime|logid|eid|ecode|ename|scan_code_order_id|scan_time_prize_id|scan_time_prize_name|" +
    "user_id|status|status_name|use_scan_time|scan_time_prize_type|scan_time_prize_amount|name|tel|address|post_code|create_time|" +
    "update_time|duijiang_status|tyopenId"

  ///<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

  override def dobuessines(rdd: RDD[String]): Unit = {

    val topicOne=filterLog(rdd,schemaString1)
    val topicTwo=filterLog(rdd,schemaString2)
    if(!topicOne.isEmpty()){
      println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>topic one >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      dobuessinesOne(topicOne)
    }
    if(!topicTwo.isEmpty()){
      println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>topic two >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      dobuessinessTwo(topicTwo)

    }
  }


  /**
    *
    * @param rdd
    */
  def dobuessinesOne(rdd: RDD[String]): Unit = {
    import hiveContext.implicits._
    //rdd.saveAsTextFile("/user/hive")
    //The schema is encoded in a string
    val schemaString="serviceId|logTime|logid|eid|ecode|ename|prizeOrderId|barcode|activityId|activityName|prizeAmount|" +
      "prizeIntegration|productId|productName|prizeType|prizeTypeName|isType|isTypeName|userId|userName|openId|isStatus|" +
      "isStatusName|isPrize|isPrizeName|prizeId|prizeName|createTime|acceptTime|getTime|isFirstGet|isFirstGetName|payOpenId|" +
      "redBagType|redBagTypeName|isActive|isActiveName|dealerId|dealerName|shopId|shopName|tyopenId"
    //Generate the schema based on  the string of schema
    val schema=generateSchema(schemaString)

    //Convert records  of the RDD (people) to Rows
    val rowRDD=generateRDDRow(filterLog(rdd,schemaString))
    //Apply the schema to the RDD
    val df=registerDF(rowRDD,schema)

    outputToHive(df,hiveContext,"prize_details_tmp")

    val shellName=registerPythonShell(sparkContext,wfc)
    //output details to mysql  and python and hbase
    val prizeDetails=df.select("prizeOrderId","productName","activityName","prizeName","isStatusName","userName","openId","isFirstGet","createTime","ecode").rdd
    outputToHbase(prizeDetails,"prize_log_details")
   // outputToPython(prizeDetails,1,shellName._1)
    val accountsDetails=df.select($"productName",$"activityName",$"createTime",$"openId",$"userName",genMoney($"prizeAmount"),$"userId",$"isStatus",$"ecode").rdd
    outputToHbase(accountsDetails,"account_details")
   // outputToPython(accountsDetails,2,shellName._2)

    logger.info(s"this window total :${df.count()}")


  }
  /**
    *
    * @param rdd
    */
  def dobuessinessTwo(rdd:RDD[String]):Unit={
    //the schema is encoded  in a string
    val schemaString="serviceId|logTime|logid|eid|ecode|ename|scan_code_order_id|scan_time_prize_id|scan_time_prize_name|" +
      "user_id|status|status_name|use_scan_time|scan_time_prize_type|scan_time_prize_amount|name|tel|address|post_code|createTime|" +
      "update_time|duijiang_status|tyopenId"
    //generate the schema base on the string schema
    val schema=generateSchema(schemaString)
    val rowRDD=generateRDDRow(filterLog(rdd,schemaString))
    //apply the schema to the RDD
    val df=registerDF(rowRDD,schema)
    outputToHive(df,hiveContext,"prize_number_details_tmp")
    //output drawNumberDetails to mysql
    val drawNumberDetails=df.select("scan_code_order_id","scan_time_prize_id","user_id","status","use_scan_time","scan_time_prize_type","scan_time_prize_amount","name","tel","address","post_code","create_time","upate_time","duijiang_status","ecode").rdd
    outputToHbase(drawNumberDetails,"drawNumberDetails")
    logger.info(s"this window total :${df.count()}")
  }

  /**
    *
    * @param rdd
    * @param tableName
    * @return
    */
  def outputToHbase(rdd:RDD[Row],tableName:String):Int={
    if(!HbaseConf.TableEixst(tableName)){
      HbaseConf.createTable(tableName)
    }
    logger.info(s"Write ++++++++++++++++++++++++++++++++++++++++++++++++to hbase table ")
    if("prize_log_details".equalsIgnoreCase(tableName)){
      val localData=rdd.map{ row=>
        val ecode=row.toSeq.last.toString
        HbaseConf.prizeConvert(row,ecode)
      }
      localData.saveAsHadoopDataset(HbaseConf.getJobConf(tableName))
    }else if("prize_number_details".equalsIgnoreCase(tableName)){
      val localData=rdd.map { row =>
        val ecode = row.toSeq.last.toString
        HbaseConf.numberConvert(row, ecode)
      }
      localData.saveAsHadoopDataset(HbaseConf.getJobConf(tableName))
    }else{
      val localData=rdd.map{ row=>
        val ecode=row.toSeq.last.toString
        HbaseConf.accountConvert(row,ecode)
      }
      localData.saveAsHadoopDataset(HbaseConf.getJobConf(tableName))
    }
    1
  }

  /**
    *
    * @param rdd
    * @param logType
    * @param shellName
    * @return
    */
  def outputToPython(rdd:RDD[Row],logType:Int,shellName:String):Int={
    val rddString=rdd.map{row=>
      row.toSeq.mkString(",")
    }
    val f=Future[RDD[String]]{
      val opData=rddString.pipe(SparkFiles.get(shellName))
      opData
    }
    f onFailure{
      case t=> logger.error("Insert to CountDetailsToExcel  failure: " +t.getMessage)
    }
    f onSuccess{
      case a=>   logger.info(s"Insert to CountDetailsToExcel successful >>>>>>>>>>>>:${a.count()}")
    }

    1
  }

  /**
    *
    * @param df
    * @param hiveContext
    * @param hiveTable
    * @param ec
    * @return
    */
  def outputToHive(df:DataFrame,hiveContext: HiveContext,hiveTable:String)(implicit ec:ExecutionContext):Int= {
    val tempTable=df.registerTempTable("df")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
   // hiveContext.sql(s"insert into ${hiveTable} partition(ee,dt) select *,ecode as ee,from_unixtime(unix_timestamp(createTime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') as dt from df ")
    hiveContext.sql(s"insert into ${hiveTable} partition(ee,dt) select *,ecode as ee,from_unixtime(unix_timestamp(),'yyyyMMdd') as dt from df ")
    1
  }

 //<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>Utils >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

  private def isEmpty( str:String):Boolean={
    val bool= (str==null || "".equals(str) || "null".equals(str))
    bool
  }

  private val genMoney=udf{prizeAmount:String=>{
    val value=if(StringUtils.isNotEmpty(prizeAmount)) (prizeAmount.toDouble / 100)   else 0.0
    value
  }}

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
  /**
    *
    * @param rdd
    * @return
    */
  private def generateRDDRow(rdd:RDD[String]):RDD[Row]={
    rdd.map{str=>
      val strSeq= str.split("\\|",-1)
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
    * @return
    */
  private def registerDF(rdd:RDD[Row],schema:StructType):DataFrame={
    import hiveContext.implicits._
    hiveContext.createDataFrame(rdd,schema)
  }
  /**
    *
    * @param sc
    * @param wfc
    * @return
    */
  private def registerPythonShell(sc:SparkContext,wfc:MainConfig):(String,String)={
    val pipeScript=s"${wfc.pioHome.getOrElse(".")}bin/PipeScript.py"
    val fileScript=s"${wfc.pioHome.getOrElse(".")}bin/file.py"
    val pipeScript1=s"${wfc.pioHome.getOrElse(".")}bin/PipeScript1.py"
  //  val pipeScript=s"hdfs://master:8020/python/shell/PipeScript.py"
   // val fileScript=s"hdfs://master:8020/python/shell/file.py"
   // val pipeScript1=s"hdfs://master:8020/python/shell/PipeScript1.py"
    val scriptName=s"PipeScript.py"
    val scriptName1=s"PipeScript1.py"

    sc.addFile(fileScript)
    sc.addFile(pipeScript)
    sc.addFile(pipeScript1)
    (scriptName,scriptName1)
  }

  /**
    *
    * @param time format:yyyy-MM-dd HH:mm:ss or yyyy-MM-dd HH:mm:ss.SSS
    * @return
    */
  private def parseTime(time:String):(Int,String)={
    val formatter=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date=try{
      formatter.parse(time)
    }catch{
      case  e:java.text.ParseException=>
        val  formatter=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        val date=formatter.parse(time)
        val a=new SimpleDateFormat("yyyyMMdd").format(date).toInt
        val b=new SimpleDateFormat("yyyy-MM-dd").format(date)
        (a,b)
    }

    val a=new SimpleDateFormat("yyyyMMdd").format(date).toInt
    val b=new SimpleDateFormat("yyyy-MM-dd").format(date)
    (a,b)
  }
}

object  SparkToHive extends  Logging{
  case class MainConfig(
                         batch:String="",
                         deployMode:String="",
                         env:Option[String]=None,
                         verbosity:Int=0,
                         test:Boolean=false,
                         verbose:Boolean=false,
                         debug:Boolean= false,
                         logFile:Option[String]=None,
                         pioHome:Option[String]=None
                       ) extends  Serializable
  val parser = new scopt.OptionParser[MainConfig]("CreateWorkflow") {
    override def errorOnUnknownArgument: Boolean = false
    opt[String]("batch") action { (x, c) =>
      c.copy(batch = x)
    } text("Batch label of the workflow run.")
    opt[String]("deploy-mode") action { (x, c) =>
      c.copy(deployMode = x)
    }
    opt[String]("env") action { (x, c) =>
      c.copy(env = Some(x))
    } text("Comma-separated list of environmental variables (in 'FOO=BAR' " +
      "format) to pass to the Spark execution environment.")
    opt[Int]("verbosity") action { (x, c) =>
      c.copy(verbosity = x)
    }
    opt[Unit]("verbose") action { (x, c) =>
      c.copy(verbose = true)
    } text("Enable verbose output.")
    opt[Unit]("test") action { (x, c) =>
      c.copy(test = true)
    } text("Enable verbose output.")
    opt[Unit]("debug") action { (x, c) =>
      c.copy(debug = true)
    } text("Enable debug output.")

    opt[String]("log-file") action { (x, c) =>
      c.copy(logFile = Some(x))
    }
    opt[String]("pio-home") action { (x, c) =>
      c.copy(pioHome = Some(x))
    } text("pio-home")

  }
  def main(args: Array[String]): Unit = {
    val wfcOpt=parser.parse(args,MainConfig())
    if(wfcOpt.isEmpty){
        logger.error("WorkflowConfig is empty Quitting")
        return
    }
    val wfc=wfcOpt.get
    val conf = new SparkConf().setAppName("UserupdateAnaysis")
    val ssc = new StreamingContext(conf, Seconds(60))
    val sc = ssc.sparkContext
    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("spark.sql.shuffle.partitions","50");

    try{
      hiveContext.sql("show tables")
    }catch {
      case e:Exception=> logger.info("CREATE EXTERNAL TABLE  user_details fail:" +e.getMessage)
        e.printStackTrace()
    }

    val process = new SparkToHive(sc,hiveContext,wfc)
    val topic1:String=ConfigurationManager.getProperty("kafka.topics01")
    val topic2:String=ConfigurationManager.getProperty("kafka.topics05")
    val topics : Set[String] =  Set(topic1,topic2)//"bdcustinfoadd3",
    val kafkaParams = Map[String, String](
        "metadata.broker.list" -> ConfigurationManager.getProperty("kafka.metadata.broker.list"),
        "group.id" -> ConfigurationManager.getProperty("kafka.group.id"),
        "auto.offset.reset" -> "smallest"
      )
    new Thread(new Runnable() {
      @Override
      override def run(): Unit ={
        try {
          logger.info("-----------------------------------------------start listen ssc start!!!")
          new MultiClientByte(ssc).StartEngine(ConfigurationManager.getInteger("stream.stop.bdbpc02"));
        } catch  {
          case e:Exception=>logger.error("---------------------------------------start listen ssc  fail:" + e.getMessage)
            e.printStackTrace();
        }
      }
    }).start()
    process.processkafka(ssc,kafkaParams,s"${ConfigurationManager.getProperty("kafka.zk.topics.dir02")}-hive", topics)
    // sc.textFile("/tmp/userlog.txt")
  }
}

