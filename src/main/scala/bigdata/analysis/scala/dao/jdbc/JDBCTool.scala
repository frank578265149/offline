package bigdata.analysis.scala.dao.jdbc

import bigdata.analysis.scala.constant.Constants
import bigdata.analysis.scala.utils.ConfigurationManager
import grizzled.slf4j.Logging


/**
  * Created by frank on 16-9-30.
  */
object JDBCTool  extends  Serializable with Logging{
  val props=Map(
    "URL"-> ConfigurationManager.getProperty(Constants.JDBC_URL),
    "USERNAME"-> ConfigurationManager.getProperty(Constants.JDBC_USER),
    "PASSWORD"-> ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
  )
  val prefix="JDBC"
  val config:StorageClientConfig=StorageClientConfig(test=false,properties = props,parallel = false)
  val client:BaseStorageClient=new StorageClient(config)
  val mysqlDao=new MysqlDao(client.client.asInstanceOf[String],client.config,prefix)
  /*val mysqlUserDao=new MysqlUserDao(client.client.asInstanceOf[String],client.config,prefix)*/
}
