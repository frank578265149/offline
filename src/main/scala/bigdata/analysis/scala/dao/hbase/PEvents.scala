package bigdata.analysis.scala.dao.hbase

/**
  * Created by frank on 16-12-26.
  */

import grizzled.slf4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

trait PEvents  extends  Serializable{
  @transient protected lazy val logger = Logger[this.type]
  def find(

          )(sc: SparkContext):RDD[Map[String,String]]


  
}
