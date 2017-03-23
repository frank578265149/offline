package bigdata.analysis.scala.dao.jdbc

import java.sql.DriverManager
import java.util.ArrayList
import java.util.HashMap
import java.util.List

import bigdata.analysis.scala.utils.JDBCHelper

import scala.collection.Map

/**
  * Created by frank on 16-10-24.
  */
object JDBCUtils {

  /** Generate 32-character random ID using UUID with - stripped */
  def generateId: String = java.util.UUID.randomUUID().toString.replace("-", "")

  def main(args: Array[String]) {
    val JDBCDriver = "com.cloudera.impala.jdbc41.Driver"
    val ConnectionURL = "jdbc:impala://114.115.213.107:9799/default"
    val query = "select count(isfirst) as 首次扫码,logid from tmp_scan_details group by logid"
    Class.forName(JDBCDriver).newInstance
    val con = DriverManager.getConnection(ConnectionURL)
    val stmt = con.createStatement()
    val rs = stmt.executeQuery(query)

     val list = JDBCHelper.ResultToListMap(rs)
    for (j <- 0 to list.size()-1){
      println(list.get(j))
    }


  }
}
