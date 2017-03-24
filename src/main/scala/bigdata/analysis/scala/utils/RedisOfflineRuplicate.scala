package bigdata.analysis.scala.utils

import java.util

import bigdata.analysis.scala.dao.IdMd5
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis._
import redis.clients.jedis.exceptions.JedisException

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
  * Created by Sigmatrix on 2017/2/14.
  */
object RedisOfflineRuplicate {
  def kafkafiter(rdd: RDD[String]): RDD[String]={
    val tmprdd =  rdd.mapPartitions((f:Iterator[String])=>{
      var result1 = new mutable.HashSet[String]()//最后要的
      var tmprdd1 = new java.util.HashSet[String]()
      val result = new HashMap[String,Response[String]]()
      val poolConfig: JedisPoolConfig = new JedisPoolConfig()
      val sentinels = new util.HashSet[String]()

      val masterName = "mymaster"
      var jedisSentinelPool:JedisSentinelPool = null
        var currentHostMaster:HostAndPort=null
        var jedis:Jedis=null
        var pipe:Pipeline=null
        var outtimesize = 60
        try{
          ConfigurationManager.getProperty("redis.sentinel").split(",").foreach(f=> sentinels.add(f))
          val outtime = ConfigurationManager.getProperty("redis.sentinel.outtime")
          val outtimekey = outtime.charAt(0)
          val outtimevalue = outtime.substring(1).toInt

        outtimekey match {
          case 'm'=> outtimesize = outtimevalue*60
            println("mmmmmmmmmmmmmmmmmmmmm" + outtimesize)
          case 'h'=> outtimesize = outtimevalue*60*60
            println("hhhhhhhhhhhhhhhhhhhhhhh" + outtimesize)
          case 'd'=>  outtimesize = outtimevalue*60*60*24
            println("dddddddddddddddddddddd" + outtimesize)
          case _=> outtimesize = 10*60
        }
        jedisSentinelPool = new JedisSentinelPool(masterName, sentinels, poolConfig)
        currentHostMaster = jedisSentinelPool.getCurrentHostMaster()
        //获取主节点的信息
        System.out.println(currentHostMaster.getHost() + "--" + currentHostMaster.getPort());
        jedis = jedisSentinelPool.getResource()
        jedis.auth(ConfigurationManager.getProperty("redis.sentinel.passwd"))
        pipe = jedis.pipelined()
      }catch {
        case e1: JedisException=>
          println("kafkafiter JedisException:" + e1.getMessage)
          SendsEmailUtils1.send("kafkafiter JedisException:请运维检查Redis服务", s"Redis过滤 调用失败1===== ${e1.getMessage}======   "+e1)
          System.exit(0)
        case e2: Exception=>
          println("kafkafiter JedisException:" + e2.getMessage)
          SendsEmailUtils1.send("kafkafiter JedisException:请运维检查Redis服务", s"Redis过滤 调用失败2===== ${e2.getMessage}======   "+e2)
          System.exit(0)
      }


     f.foreach(f1=>{
       val v = IdMd5.Bit16(f1)
       // result1.add(f1)
        tmprdd1.add(f1)
        result.put(v, pipe.get(v))
      })
      pipe.sync()

      val ritor = tmprdd1.iterator
      var m = 0
      var m1 = 0
      while (ritor.hasNext){
        val k0 = ritor.next()
        val kv =  IdMd5.Bit16(k0)
        val v1 = result.get(kv).get.get()
        if(v1 != "1"){
          //存储到redis并做为要处理的数据
          pipe.setex(kv, outtimesize, "1")
          m1 = m1 +1
        }else{
          result.remove(kv)
          ritor.remove()
          m = m +1
        }
      }
      pipe.close()
      jedis.close()
      jedisSentinelPool.close()
      tmprdd1.asScala.toIterator
    }
    )
    tmprdd
  }


  def kafkafiterPrize(rdd: RDD[String]): RDD[String]={
    val offlineKey="o11"
    val tmprdd =  rdd.mapPartitions((f:Iterator[String])=>{
      var result1 = new mutable.HashSet[String]()//最后要的
      var tmprdd1 = new java.util.HashSet[String]()
      val result = new HashMap[String,Response[String]]()
      val poolConfig: JedisPoolConfig = new JedisPoolConfig()
      val sentinels = new util.HashSet[String]()

      val masterName = "mymaster"
      var jedisSentinelPool:JedisSentinelPool = null
      var currentHostMaster:HostAndPort=null
      var jedis:Jedis=null
      var pipe:Pipeline=null
      try{
        ConfigurationManager.getProperty("redis.sentinel").split(",").foreach(f=> sentinels.add(f))
        val outtime = ConfigurationManager.getProperty("redis.sentinel.outtime")
        val outtimekey = outtime.charAt(0)
        val outtimevalue = outtime.substring(1).toInt
        var outtimesize = 60
        outtimekey match {
          case 'm'=> outtimesize = outtimevalue*60
          case 'h'=> outtimesize = outtimevalue*60*60
          case 'd'=>  outtimesize = outtimevalue*60*60*24
          case _=> outtimesize = 10*60
        }
        jedisSentinelPool = new JedisSentinelPool(masterName, sentinels, poolConfig)
        currentHostMaster = jedisSentinelPool.getCurrentHostMaster()
        //获取主节点的信息
        System.out.println(currentHostMaster.getHost() + "--" + currentHostMaster.getPort());
        jedis = jedisSentinelPool.getResource()
        jedis.auth(ConfigurationManager.getProperty("redis.sentinel.passwd"))
        pipe = jedis.pipelined()
      }catch {
        case e1: JedisException=>
          println("kafkafiter JedisException:" + e1.getMessage)
          SendsEmailUtils1.send("kafkafiter JedisException:请运维检查Redis服务", s"Redis过滤 调用失败1===== ${e1.getMessage}======   "+e1)
          System.exit(0)
        case e2: Exception=>
          println("kafkafiter JedisException:" + e2.getMessage)
          SendsEmailUtils1.send("kafkafiter JedisException:请运维检查Redis服务", s"Redis过滤 调用失败2===== ${e2.getMessage}======   "+e2)
          System.exit(0)
      }


      f.foreach(f1=>{
        var f1Array: Array[String] = f1.split("\\|", -1)
        if(f1Array(21)=="0"||f1Array(21)=="4"){
            val v  = offlineKey+f1Array(1)+f1Array(4)+f1Array(6)+f1Array(21)+f1Array(30)
            if(result.get(v)==None){
              result.put(v, pipe.get(v))
              tmprdd1.add(f1)
            }
        }else{
          val v  = offlineKey+f1Array(4)+f1Array(6)+f1Array(21)
          if(result.get(v)==None){
            tmprdd1.add(f1)
            result.put(v, pipe.get(v))
          }
        }
      })
      pipe.sync()

      val ritor = tmprdd1.iterator
      var m = 0
      var m1 = 0


      while (ritor.hasNext){
        val k0 = ritor.next()
        var f1Array: Array[String] = k0.split("\\|", -1)
        var kv = ""
        if(f1Array(21)=="0"||f1Array(21)=="4"){
          kv  = offlineKey+f1Array(1)+f1Array(4)+f1Array(6)+f1Array(21)+f1Array(30)
          var  v1 = result.get(kv).get.get()
          if(v1 != "1"){
            //存储到redis并做为要处理的数据
            pipe.setex(kv, 10*60, "1")
            m1 = m1 +1
          }else{
            result.remove(kv)
            ritor.remove()
            m = m +1
          }
        }else{
          kv  = offlineKey+f1Array(4)+f1Array(6)+f1Array(21)
          val v1 = result.get(kv).get.get()
          if(v1 != "1"){
            //存储到redis并做为要处理的数据
            pipe.setex(kv, 10*60, "1")
            m1 = m1 +1
          }else{
            result.remove(kv)
            ritor.remove()
            m = m +1
          }
        }
      }
      pipe.close()
      jedis.close()
      jedisSentinelPool.close()
      tmprdd1.asScala.toIterator
    }
    )
    tmprdd
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("tttttt").setMaster("local[2]")
    val sc = new SparkContext(conf)

    var data:Seq[(String)] = Nil
    for(i<-1 to 100){
      data = data ++ Seq(i.toString+"a")
    }


    val rdd = sc.makeRDD(data).repartition(1)
    println(kafkafiter(rdd).count())
    val poolConfig: JedisPoolConfig = new JedisPoolConfig()
    val sentinels = new util.HashSet[String]()
   /* sentinels.add("dev.bi-report.v5q.cn:9381")
    sentinels.add("dev.bi-report.v5q.cn:9382")
    sentinels.add("dev.bi-report.v5q.cn:9383")*/
     ConfigurationManager.getProperty("redis.sentinel").split(",").foreach(f=> sentinels.add(f))

    val masterName = "mymaster"
    val jedisSentinelPool = new JedisSentinelPool(masterName, sentinels, poolConfig)
    val currentHostMaster = jedisSentinelPool.getCurrentHostMaster()
    System.out.println(currentHostMaster.getHost() + "--" + currentHostMaster.getPort());
    //获取主节点的信息
    val resource: Jedis = jedisSentinelPool.getResource()
    resource.auth(ConfigurationManager.getProperty("redis.sentinel.passwd"))
    val outtime = ConfigurationManager.getProperty("redis.sentinel.outtime")
    val outtimekey = outtime.charAt(0)
    val outtimevalue = outtime.substring(1).toInt
    var outtimesize = 60
    outtimekey match {
      case 'm'=> outtimesize = outtimevalue*60
      case 'h'=> outtimesize = outtimevalue*60*60
      case 'd'=>  outtimesize = outtimevalue*60*60*24
      case _=> outtimesize = 10*60
    }

    val map=new HashMap[String, String]();
   for(k<-950 to 1050)
    map.put(k.toString, k.toString);

    val  result = new HashMap[String,Response[String]]()
    val pipe = resource.pipelined()
    val imap = map.iterator

    while (imap.hasNext){
      val kv = imap.next()
     // pipe.setex(kv._1.toString, 30*60,kv._2)
      result.put(kv._1, pipe.get(kv._2))

     // println(kk.get())
    }
   // println("re"+ result.size)

    pipe.sync()
    val ritor = result.iterator
    while (ritor.hasNext){
      val kv = ritor.next()
      val v1 = result.get(kv._1).get.get()
      if(v1 == null){
        //存储到redis并做为要处理的数据
        pipe.setex(kv._1, outtimekey, "1")
        println(result.get(kv._1).get.get())
      }else{
        println(result.get(kv._1).get.get())
        result.remove(kv._1)
      }
    }




   // val p = pipe.get("mmm")
  //  pipe.sync()
 //   println(resource.get("mmm"))
    //println( p.get())

    println("")
   // println("ffffffffffff" + pipe.get("mm").get())
  }
}
