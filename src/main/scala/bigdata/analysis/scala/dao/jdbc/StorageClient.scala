package bigdata.analysis.scala.dao.jdbc

import scalikejdbc._

/**
  * Created by frank on 16-7-18.
  */
class StorageClient(val config:StorageClientConfig)  extends  BaseStorageClient/* with Logging*/{
  override  val prefix="JDBC"

  if (!config.properties.contains("URL")) {
    throw new StorageClientException("The URL variable is not set!", null)
  }
  if (!config.properties.contains("USERNAME")) {
    throw new StorageClientException("The USERNAME variable is not set!", null)
  }
  if (!config.properties.contains("PASSWORD")) {
    throw new StorageClientException("The PASSWORD variable is not set!", null)
  }
  // set max size of connection pool
  val maxSize: Int = config.properties.getOrElse("CONNECTIONS", "8").toInt
  val settings = ConnectionPoolSettings(maxSize = maxSize)
  ConnectionPool.singleton(
    config.properties("URL"),
    config.properties("USERNAME"),
    config.properties("PASSWORD"),
    settings)
  /** JDBC connection URL. Connections are managed by ScalikeJDBC. */
  val client = config.properties("URL")
}
