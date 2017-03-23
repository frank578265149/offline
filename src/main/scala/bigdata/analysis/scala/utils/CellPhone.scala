package bigdata.analysis.scala.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by frank on 16-10-17.
  */
case class CellInfo(
                   id:Long=50,
                   osType:String="",
                   phoneBrand:String="",
                   phoneModel:String="",
                   scanSum:Int=0,
                   userSum:Int=0,
                   userAgent:String=""
                   ){
  override def toString: String =s"${osType}::${phoneModel}::${userAgent}"
}
object CellPhone {
  def main(args: Array[String]) {
    val reg=""".*(\([A-Za-z]*);(.*\))""".r
    val conf=new SparkConf()
    conf.setMaster("local").setAppName("cell-phone")
    val sc=new  SparkContext(conf)
    val accessLog=sc.textFile("E:/flie/access_xmht.log-20161016")
    process(accessLog).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).take(100).foreach(println)
    processDetails(accessLog).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).take(100).foreach(println)
    accessLog.map{
      line=>
        line.split("\\|",-1).apply(5)
    }.map(getCellInfo(_)).foreach(println)
  }
  def process(accessLog:RDD[String]):RDD[String]={
    val rawRDD=accessLog.map{line=>
      line.split("\\|",-1).apply(5)
    }
    val processLog=rawRDD.map{ str=>
      try{
        str.split("\\(").apply(1).split("\\)").apply(0)
      }catch{
        case e:Throwable=>
          "other"
      }
    }
    processLog.map{str=>
      val strSeq=str.split(" ")
      if(strSeq(0).trim.equalsIgnoreCase("Linux;")){
        "andriod"
      }else if(strSeq(0).trim.equalsIgnoreCase("iPhone;")){
        "iphone"
      }else if(strSeq(0).trim.equalsIgnoreCase("Windows") || strSeq(0).trim.equalsIgnoreCase("Macintosh")){
        "website"
      }else{
        "other"
      }
    }
  }
  def processDetails(accessLog:RDD[String]):RDD[Any]={
    val rawRDD=accessLog.map(_.split("\\|",-1).apply(5))
    val processLog=rawRDD.map{ str=>
      try{
        str.split("\\(").apply(1).split("\\)").apply(0)
      }catch{
        case e:Throwable=>
         "other"
      }
    }
    processLog.map{str=>
      val strSeq=str.split(" ")
      if(strSeq(0).trim.equalsIgnoreCase("Linux;")){
        strSeq.drop(3).dropRight(1).mkString(":").trim
      }else if(strSeq(0).trim.equalsIgnoreCase("iPhone;")){
        strSeq.drop(3).dropRight(4).mkString(":").trim
      }else if(strSeq(0).trim.equalsIgnoreCase("Windows") || strSeq(0).trim.equalsIgnoreCase("Macintosh")){
        "website"
      }else{
        "other"
      }
    }
  }
  def getCellInfo(str:String):CellInfo={
    val strA=try{
      str.split("\\(").apply(1).split("\\)").apply(0)
    }catch{
      case e:Throwable=>
        "other"
    }
    val strSeq=strA.split(" ")
    val cellInfo=
      if(strSeq(0).trim.equalsIgnoreCase("Linux;")){

            val phoneModel=strSeq.drop(3).dropRight(1).mkString("-").trim
           CellInfo(osType = "android",phoneModel =phoneModel,scanSum = 0,userSum = 0,userAgent = strA )
      }else if(strSeq(0).trim.equalsIgnoreCase("iPhone;")){

           val phoneModel=strSeq.drop(3).dropRight(4).mkString("-").trim
           CellInfo(osType="os",phoneModel=phoneModel,scanSum = 0,userSum = 0,userAgent = strA)
     }else if(strSeq(0).trim.equalsIgnoreCase("Windows") || strSeq(0).trim.equalsIgnoreCase("Macintosh")){

           CellInfo(osType="win|mac",phoneModel="win|mac",scanSum = 0,userSum = 0,userAgent = strA)
     }else{

           CellInfo(osType="other",phoneModel="other",scanSum = 0,userSum = 0,userAgent = strA)
     }
    cellInfo
  }

}
