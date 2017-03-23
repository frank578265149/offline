/*
package bigdata.analysis.java.kafka


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import bigdata.analysis.java.core.UserAnalysis
import bigdata.analysis.java.dao.{HIVEContextSingleton, KafkaToSpark}

/**
  * Created by Sigmatrix on 2016/10/8.
  */
class Testkafka extends KafkaToSpark{
  override def dobuessines(rdd: RDD[String]): Unit = {

    val schemaString="prize_order_id::barcode"

    //Generate the schema based on  the string of schema
    val fields = schemaString.split("::").
      map(filedName=> StructField(filedName,StringType,nullable = true))
    val schema=StructType(fields)
    // Convert RDD[String] to RDD[case class] to DataFrame

    val rowRDD = rdd.map(_.split(",")).map(p => Row(p(0).trim, p(1).trim))
    rowRDD.count()
    val sqlContext = HIVEContextSingleton.getInstance(rdd.sparkContext)
    val userdf = sqlContext.createDataFrame(rowRDD, schema)

    userdf.show()

    println("*****************************************************" + rowRDD.count())
    rdd.foreachPartition(
      message => {
        while(message.hasNext) {
          println(s"@^_^@   [" + message.next() + "] @^_^@")
        }
      }
    )
  }
}

object Testkafka {
  def main(args: Array[String]): Unit = {
    val process = new UserAnalysis

    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(10))
    val topic : String = "xmhttopic"   //消费的 topic 名字
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "192.168.149.141:9092",
      "group.id" -> "groupId0111",
      "auto.offset.reset" -> "largest"
    )
   //////
    process.processkafka(ssc, kafkaParams, "test_spark_streaming_group2", topic)
  }
}
*/
