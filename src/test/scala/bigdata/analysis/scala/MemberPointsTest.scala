package bigdata.analysis.scala

import java.text.SimpleDateFormat

import bigdata.analysis.scala.utils.Getip
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ArrayBuffer
import scala.tools.scalap.scalax.util.StringUtil
import scala.util.Random

/**
  * Created by frank on 17-2-15.
  */
class MemberPointsTest extends FunSuite with Matchers  with SharedSparkContext  {

  test("producing memberpoints test data"){
    val base=1 to(100)
    val base1=base.map{ index=>
      Seq(
        index,//serviceId
        getTime(),//logTime
        index,//logId
        "test01",//ecode
        "ecodename",//ename
        1,//userId
        2,//activityId
        "source",//source
        "source-name",//sourcename
        3.5,//score
        getTime(),//time
        index,//openid
        getTime(),//tyopenid
        "114.115.213.107"//ip

      ).mkString("|")
    }

    val sed=new Random()
    val data=sc.makeRDD(base1)
    data.saveAsTextFile("/home/frank/IdeaProjects/tracing-source/log/member")
  }
  def getId():String={
    java.util.UUID.randomUUID().toString.replace("-","")
  }
  def getTime():String={
    val formatTime=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis())
    formatTime
  }
}