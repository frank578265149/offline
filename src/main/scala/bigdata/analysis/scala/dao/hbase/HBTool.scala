package bigdata.analysis.scala.dao.hbase

import bigdata.analysis.scala.constant.Constants
import bigdata.analysis.scala.dao.jdbc.{BaseStorageClient, StorageClientConfig}
import bigdata.analysis.scala.utils.ConfigurationManager
import grizzled.slf4j.Logging
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by frank on 16-12-26.
  */
object HBTool extends  Serializable with  Logging{
  val props=Map(
    "URL"-> ConfigurationManager.getProperty(Constants.JDBC_URL),
    "USERNAME"-> ConfigurationManager.getProperty(Constants.JDBC_USER),
    "PASSWORD"-> ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
  )
  val prefix="HB"
  val config:StorageClientConfig=StorageClientConfig(test=false,properties = props,parallel = true)
  val client:BaseStorageClient=new StorageClient(config)
  val hbaseDao=new HBPEvents(client.client.asInstanceOf[HBClient],client.config,prefix)

  def main (args: Array[String]) {
    val sparkConf=new SparkConf().setMaster("local").setAppName("pio")
    val sc=new SparkContext(sparkConf)
    hbaseDao.checkTableExists()
    System.currentTimeMillis()
    val rdd=hbaseDao.find()(sc)

    rdd.foreach(userMap=>

      userMap.keySet.map{key=>

        println(s"key:${key}------>${userMap.get(key)}")
      }


    )
  }

}
