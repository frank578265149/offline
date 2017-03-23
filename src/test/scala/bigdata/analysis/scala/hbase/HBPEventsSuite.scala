package bigdata.analysis.scala.hbase

import java.util.Random

import bigdata.analysis.scala.SharedSparkContext
import bigdata.analysis.scala.dao.hbase.{HBASETool, HBTool, HbaseConf}
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.FunSuite
import org.scalatest.Matchers
/**
  * Created by frank on 17-1-6.
  */
class HBPEventsSuite  extends FunSuite with Matchers with  SharedSparkContext {

/*
  test("insert data to hbase "){
    val seq=1.to(10000)
    val rdd=sc.makeRDD(seq)
    val random=new Random(100)
    val d=rdd.map {e=>

      Map(     "user_id" -> s"frank-${e}",
        "ip" -> "frank",
        "hyd" -> s"${random.nextInt(100)}",
        "js" -> s"${random.nextInt(100)}",
        "th" -> s"${random.nextInt(100)}",
        "2bqn" ->s"${random.nextInt(100)}",
        "nqds" -> s"${random.nextInt(100)}")

    }
    val localData=d.map{dataMap=>
      TestUtil.mapConvert(dataMap)
    }
    localData.saveAsHadoopDataset(HbaseConf.getJobConf("pio_test"))
    assert(4==4)
  }
  test("load data from hhbase "){
    val rdd=  HBTool.hbaseDao.find()(sc)
    println(s">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>${rdd.count()}")
    rdd.foreach{ userMap=>
       userMap.keys.foreach(key=>
         println(s"key:${key}------------------------------value:${userMap.getOrElse(key,"0")}")

       )

    }
  }*/
  test("tracing core  get search AreaId by ProvinceId "){
    val provinceId="110000"
    val ecode:String="sjf001"
    val tableName1="ent_area_sales_area_rela"

    val family1="traceFamily"
    // the column  that add filter
    val qualifier11="area_level1_id"
    // the column that we want to get
    val qualifier12="sales_area_code"
    var bigCode=""
    val result1=HBASETool.hbaseProxy.scanRowByValue(tableName1,family1,qualifier11,provinceId)
    if(!result1.isEmpty){
      val salesAreaCode=Bytes.toString(result1.get(0).getValue(Bytes.toBytes(family1),Bytes.toBytes(qualifier12)))//QDZLa31006
      println(s"<<<<<<<<<<<<<sales_area_code...${salesAreaCode}")
      val tableName2="ent_sales_area"
      val family2="traceFamily"
      val qualifier2="parentCode"
      val rowkey2=s"$ecode|$salesAreaCode"
      val result2=HBASETool.hbaseProxy.getRowByRowKey(tableName2,family2,qualifier2,rowkey2)
      if(!result2.isEmpty){
        val parentCode=Bytes.toString(result2.getValue(Bytes.toBytes(family2),Bytes.toBytes(qualifier2)))
        bigCode=parentCode

      }
    }


    println(s"bigcode>>>>>>>>>>>>>>>>>>>>>>>>>>${bigCode}")


  }
  
}
