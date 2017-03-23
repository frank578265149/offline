package bigdata.analysis.scala.dao

import bigdata.analysis.scala.utils.ConfigurationManager
import grizzled.slf4j.{Logger, Logging}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.I0Itec.zkclient.exception.ZkTimeoutException
import org.I0Itec.zkclient.serialize.SerializableSerializer
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

/**
  * Created by Sigmatrix on 2016/10/8.
  */
abstract class KafkaToSpark{
  @transient
  protected  lazy val logger=Logger[this.type ]
  /*
  **用户实现dobuessines接口，rdd就是一组日志
   */
  def dobuessines(rdd:RDD[String])

  def processkafka(ssc:StreamingContext, kafkaParams:Map[String, String],
                   zkTopicDirs:String, topics: Set[String]): Unit ={

    val ZKServers: String =  ConfigurationManager.getProperty("kafka.zkservers")
    val brokers:String =  ConfigurationManager.getProperty("kafka.metadata.broker.list")
    var zkClient:ZkClient = null

    try{
      zkClient = new ZkClient(ZKServers,30000, 30000)
      logger.info("zkclient init complete!!!!!")
    }catch {
      case ex:ZkTimeoutException => logger.error("zkclient time out!")
      case e:Exception=>{
        logger.error("zkclint Exception : " + e.getMessage)
        logger.error("kafka.zkservers ============" + ZKServers)
        logger.error("kafka.metadata.broker.list==========" + brokers)
      }
    }
    val zkUtils = new ZkUtils(zkClient, null, false)
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    var topic_ZKGroupTopicDirs:Map[String, ZKGroupTopicDirs] = Map()//用于zk保存时根据topic找到对应的ZKGroupTopicDirs
    var kafkaStream : InputDStream[(String, String)] = null
    var isfirst = false
    //创建一个 ZKGroupTopicDirs 对象，对保存
    var topicDirset = for(i<-topics) yield {
      val zktopdir =   new ZKGroupTopicDirs(zkTopicDirs,  i)
      topic_ZKGroupTopicDirs += (i-> zktopdir)
      zktopdir
    }
    for(i<-topicDirset){
      val child = zkClient.countChildren(s"${i.consumerOffsetDir}")
      if(child >0) isfirst = true
    }

    try{
      if(isfirst){
        for(topicDir<-topicDirset){
          //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
          val children = zkClient.countChildren(s"${topicDir.consumerOffsetDir}")
          if (children > 0) {   //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
            for (i <- 0 until children) {
              val partitionOffset = zkClient.readData[String](s"${topicDir.consumerOffsetDir}/${i}")
              val tmp = topicDir.consumerOffsetDir.split("/")
              val topic = tmp(tmp.length-1)
              val tp = TopicAndPartition(topic, i)
              fromOffsets += (tp -> partitionOffset.toLong)  //将不同 partition 对应的 offset 增加到 fromOffsets 中
            }
          }
        }
        val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())  //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
        kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
      }//end isfirst
      else {
        kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics) //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
      }
    }catch {
      case e:ZkTimeoutException=>
        logger.error("ZkTimeoutException:"+e.getMessage)
        e.printStackTrace()
      case e2:Exception =>
        logger.error("KafkaToSpark: "+e2.getMessage)
        e2.printStackTrace()
    }

    var offsetRanges = Array[OffsetRange]()
    kafkaStream.transform{ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.map(msg => msg._2).foreachRDD { rdd =>
      if (!rdd.isEmpty()){
        try{
          dobuessines(rdd)//先处理业务逻辑
          //业务处理完后更新偏移量
          for (o <- offsetRanges) {
            val topDir = topic_ZKGroupTopicDirs(o.topic)
            val zkPath = s"${topDir.consumerOffsetDir}/${o.partition}"
            //  ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)  //将该 partition 的 offset 保存到
          //  zkUtils.updatePersistentPath(zkPath, o.untilOffset.toString)
            // zookeeper
            logger.info(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######")
          }
        }catch {
          case ex:Exception=>ex.printStackTrace()
        }

      }
    }
    ssc.start()

    ssc.awaitTermination()
  }
}
