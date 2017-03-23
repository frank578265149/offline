package bigdata.analysis.scala.dao.hbase

import bigdata.analysis.scala.dao.jdbc.StorageClientConfig
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Delete, HTable, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{PIOHBaseUtil, TableInputFormat, TableOutputFormat}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
  * Created by frank on 16-12-26.
  */
class HBPEvents(client:HBClient,config:StorageClientConfig,namespace:String)  extends  PEvents{

  def checkTableExists():Unit={
    if(!client.admin.tableExists("rio_tmp")){
        logger.error(s"hbase table not found ")
        throw new Exception(s"HBase table not found ")
    }
    logger.info(s"hbase table is exists")
  }
  def find(

          )(sc: SparkContext):RDD[Map[String,String]]={
    checkTableExists()
    val conf=HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "rio_tmp")
    val scan=HBEventsUtil.createScan(
    )
    scan.setCaching(500) // TODO
    scan.setCacheBlocks(false) // TODO
    conf.set(TableInputFormat.SCAN, PIOHBaseUtil.convertScanToString(scan))
    // HBase is not accessed until this rdd is actually used.
    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map {
      case (key, row) => HBEventsUtil.resultToEvent(row)
    }
    rdd
  }

}
