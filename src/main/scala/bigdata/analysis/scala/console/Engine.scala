package bigdata.analysis.scala.console

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by frank on 16-12-13.
  */
class Engine(val sparkContext: SparkContext,val hiveContext: HiveContext) {
  val rdd=sparkContext.textFile("/home/frank/IdeaProjects/tracing-source/log/goodscheck")
  def  searchDetails(command:String):String={
    val success=hiveContext.sql(command).toJSON
    success.first()

  }

  
}
