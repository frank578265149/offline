package bigdata.analysis.scala

import java.io.File
import java.nio.charset.StandardCharsets

import bigdata.analysis.scala.utils.Utils
import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.scalatest.{FunSuite, Matchers}
import java.io.File

import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
/**
  * Created by frank on 17-3-23.
  */
class HcfsTest extends FunSuite with Matchers with SharedSparkContext{
  test("hdoop comptiable file system "){
    val tempDir=Utils.createTempDir()
    val sourceDir=new File(tempDir,"source-dir")
    val innerSourceDir=Utils.createTempDir(root=sourceDir.getPath)
    val sourceFile=File.createTempFile("someprefix","somesuffix",innerSourceDir)
    val targetDir = new File(tempDir, "target-dir")
    Files.write("some text", sourceFile, StandardCharsets.UTF_8)
    val path=
      if(Utils.isWindows){
        new Path("file:/" + sourceDir.getAbsolutePath.replace("\\","/"))
      }else{
        new Path("file://" + sourceDir.getAbsolutePath)
      }
    val conf=new Configuration()
    val fs=Utils.getHadoopFileSystem(path.toString,conf)
    Utils.fetchHcfsFile(path,targetDir,fs,new SparkConf(),conf,false)
    assert(targetDir.isDirectory)


  }
  test("Kill process"){
    if(SystemUtils.IS_OS_LINUX){

    }
  }
}
