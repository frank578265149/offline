package bigdata.analysis.scala.dao

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import  org.apache.spark.sql.hive.HiveContext

/**
  * Created by Sigmatrix on 2016/10/9.
  */
/** Lazily instantiated singleton instance of SQLContext */
object HIVEContextSingleton {
  @transient private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      println("instance come in..........................................")
      instance = new SQLContext (sparkContext)
    }
    else {
      println("instance not come in..........................................")
    }
    instance
  }
  @transient private var instancehive: HiveContext = _
  def getInstancehive(sparkContext: SparkContext): HiveContext = {
    if (instancehive == null) {
      println("instance come in..........................................")
      instancehive = new HiveContext (sparkContext)
    }
    else {
      println("instance not come in..........................................")
    }
    instancehive
  }
}
