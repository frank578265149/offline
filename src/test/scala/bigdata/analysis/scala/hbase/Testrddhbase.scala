/*
package bigdata.analysis.java.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sigmatrix on 2016/10/12.
  */
object Testrddhbase {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val sc = new SparkContext(conf)
    val readFile =   sc.textFile("file/log/customer_info.log")
    .map{x =>
      val y1 = x.split("\\|")
      (y1(3),y1(1))
    }

    val tableName = "member"
    readFile.foreachPartition{
      x=> {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "192.168.149.141,192.168.149.142,192.168.149.143")
        myConf.set("hbase.zookeeper.property.clientPort", "2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, TableName.valueOf(tableName))
        myTable.setAutoFlush(false, false)//关键点1
        myTable.setWriteBufferSize(3*1024*1024)//关键点2
        x.foreach {
         case(x, y )=> {
          println(x + ":::" + y)

          val p = new Put(Bytes.toBytes(y))

          p.add("member_id".getBytes, "qualifier1".getBytes, Bytes.toBytes(x))
          myTable.put(p)
        }
        }
        myTable.flushCommits()//关键点3
      }
    }
  }
}
*/
