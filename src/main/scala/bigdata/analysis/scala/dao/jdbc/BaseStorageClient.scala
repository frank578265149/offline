package bigdata.analysis.scala.dao.jdbc

import scala.language.existentials

/**
  * Created by frank on 16-7-18.
  */
trait  BaseStorageClient{
  val config:StorageClientConfig
  val client:AnyRef
  val prefix:String=""
}





