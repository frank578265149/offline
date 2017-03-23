package bigdata.analysis.scala.dao

import bigdata.analysis.scala.utils.ConfigurationManager
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

/**
  * Created by Sigmatrix on 2016/10/12.
  */
object UserHbaseDao {
  def gethbaserdd(df:DataFrame): RDD[(String, Seq[Any])] ={
    val rdd = df.rdd
    rdd.map(row=>{
      (
        row.getAs[String]("ecode")+"|" +  row.getAs[String]("logTime"),     //rowkey
        Seq( "createTime"->row.getAs[String]("createTime"),"type"->row.getAs[String]("type"),"openId"->row.getAs[String]("openId"),
          "phone"->row.getAs[String]("phone"),"birthYear"->row.getAs[String]("birthYear"),"sex"->row.getAs[String]("sex"),
          "Provincename"->row.getAs[String]("Provincename"),"Cityname"-> row.getAs[String]("Cityname"),"activityName"->row.getAs[String]("activityName"))
        )
    })
  }

  def tohbase(rdd:RDD[(String, Seq[Any])], table_name: String): Unit ={
    rdd.foreachPartition{
      itor=> {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", ConfigurationManager.getProperty("kafka.zkservers"));
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(conf, table_name)
        myTable.setAutoFlush(false, false)    //关键点1
        myTable.setWriteBufferSize(3*1024*1024)//关键点2
        itor.foreach {
          case(x, y )=> {
            //   println(x + ":::" + y)
            val row = new Put(Bytes.toBytes(x))
            y.foreach{
              case (qualifiername, qualifier) =>
                row.addColumn("userFamily".getBytes, qualifiername.asInstanceOf[String].getBytes(),Bytes.toBytes(qualifier.asInstanceOf[String]))
            }
            myTable.put(row)
          }
        }
        myTable.flushCommits()//关键点3
        myTable.close()
      }
    }
  }

}
