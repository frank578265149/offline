/*
package bigdata.analysis.java.kafka

import bigdata.analysis.java.utils.ConfigurationManager

import bigdata.analysis.java.dao.KafkaToSpark
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Sigmatrix on 2016/10/18.
  */
class Testlinekafka  extends KafkaToSpark {
  override def dobuessines(rdd: RDD[String]): Unit = {
     rdd.foreachPartition(f=>f.foreach(println(_)))
  }
}

object Testlinekafka {
  def main(args: Array[String]): Unit = {
    val process = new Testlinekafka

    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(10))
    val topic : String =ConfigurationManager.getProperty("kafka.topics01")   //消费的 topic 名字
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> ConfigurationManager.getProperty("kafka.metadata.broker.list"),
      "group.id" -> "groupId01",
      "auto.offset.reset" -> "largest"
    )
    //////
    process.processkafka(ssc, kafkaParams, "test_spark_streaming_group2", topic)
  }

}
*/
