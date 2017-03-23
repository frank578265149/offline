package bigdata.analysis.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

/**
  * Created by frank on 17-3-23.
  */
class S3Test extends FunSuite with Matchers {
  test("read content from s3 "){
    val conf=new SparkConf().setAppName("simple application")
      .setMaster("local")
    val sc=new SparkContext(conf)
    sc.hadoopConfiguration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.hadoopConfiguration.set("fs.s3a.access.key", "AKIAPFQQLQSW4CMZMIZQ")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "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")
    sc.hadoopConfiguration.set("fs.s3a.endpoint","https://s3.cn-north-1.amazonaws.com.cn")
    val logFile="s3a://rio-bucket/dev/tt.txt"
    val logData=sc.textFile(logFile)
    logData.foreach(println(_))
    sc.stop
  }
}
