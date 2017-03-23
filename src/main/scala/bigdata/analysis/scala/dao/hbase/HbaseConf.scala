package bigdata.analysis.scala.dao.hbase

import bigdata.analysis.scala.constant.Constants
import bigdata.analysis.scala.utils.ConfigurationManager
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.Row

/**
  * Created by frank on 16-10-11.
  */
object HbaseConf {
  //Connection
  val conf=HBaseConfiguration.create()

  if(ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)){
    conf.set("hbase.zookeeper.property.clientPort", ConfigurationManager.getProperty("hbase.zookeeper.quorun.port"))
    conf.set("hbase.zookeeper.quorum", ConfigurationManager.getProperty("hbase.zookeeper.quorum.servers"))
  }

  val admin=new HBaseAdmin(conf)
  //list the tables
  def printTable():Unit={
    val listTables=admin.listTables()
    listTables.foreach(println(_))
  }
  def createTable(tableName:String):Int={
    val desc=new HTableDescriptor(tableName)
    desc.addFamily(new HColumnDescriptor("abbr"))
    admin.createTable(desc)
    1
  }
  def TableEixst(tableName:String):Boolean={
    admin.tableExists(tableName)
  }
  def numberConvert(record:Row,ecode:String)={
    val rowKey=System.currentTimeMillis() +ecode
    val family=Bytes.toBytes("abbr")
    val schema:Seq[String]=Vector("scan_code_order_id","scan_time_prize_id","user_id","status","use_scan_time","scan_time_prize_type","scan_time_prize_amount","name","tel","address","post_code","create_time","upate_time","duijiang_status","ecode")
    val p=new Put((Bytes.toBytes(rowKey)))
    for(i <- 0 until schema.size){
      val column= Bytes.toBytes(schema(i))
      val value=Bytes.toBytes(record(i).toString())
      p.add(family,column,value)
    }

    (new ImmutableBytesWritable,p)
  }
  def prizeConvert(record:Row,ecode:String)={
    val rowKey=System.currentTimeMillis() +ecode
    val family=Bytes.toBytes("abbr")
    val schema:Seq[String]=Vector("prizeOrderId","productName","activityName","prizeName","isStatusName","userName","openId","isFirstGet","createTime","ecode")
    val p=new Put((Bytes.toBytes(rowKey)))
    for(i <- 0 until schema.size){
      val column= Bytes.toBytes(schema(i))
      val value=Bytes.toBytes(record(i).toString())
      p.add(family,column,value)
    }

    (new ImmutableBytesWritable,p)
  }
//对账详情
  def accountConvert(record:Row,ecode:String)={
    val rowKey=System.currentTimeMillis() +ecode
    val family=Bytes.toBytes("abbr")
    val schema:Seq[String]=Vector("productName","activityName","userName","openId","prizeTime","prizeAmount")
    val p=new Put((Bytes.toBytes(rowKey)))
    for(i <- 0 until schema.size){
      val column= Bytes.toBytes(schema(i))
      val value=Bytes.toBytes(record(i).toString())
      p.add(family,column,value)
    }

    (new ImmutableBytesWritable,p)
  }

  def getJobConf(tableName:String):JobConf={
    val jobConf=new JobConf(conf,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    jobConf
  }
}
