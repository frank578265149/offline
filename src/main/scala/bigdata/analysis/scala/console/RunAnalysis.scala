package bigdata.analysis.scala.console

import java.io.File
import java.net.URI

/**
  * Created by frank on 16-10-19.
  */
object RunAnalysis  {
  def  newRunWorkflow(ca:ConsoleArgs,em:EngineManifest):Int={
    val pioHome=ca.common.pioHome.getOrElse("")
    val jarFiles=if(new File(pioHome+File.separator+"RELEASE").exists()){
      Seq(s"file://${pioHome}/lib/mysql-connector-java-5.1.36.jar").map(new URI(_))
    }else{
      Nil
    }
    val args=Seq("--verbosity", "0") ++
      (if(ca.common.test) Seq("--test") else Seq()) ++
      ca.common.pioHome.map(x=>Seq("--pio-home",x)).getOrElse(Seq()) ++
      (if(ca.common.verbose) Seq("--verbose") else Seq())

    Runner.runOnSpark(
      "bigdata.analysis.scala.core.ConsumerProfile",
      args,
      ca,
      jarFiles)
  }
  def  newRunMysqlWorkflow(ca:ConsoleArgs,em:EngineManifest):Int={
    val pioHome=ca.common.pioHome.getOrElse("")
    val jarFiles=if(new File(pioHome+File.separator+"RELEASE").exists()){
      Seq(s"file:${pioHome}/lib/mysql-connector-java-5.1.36.jar").map(new URI(_))
    }else{
      Nil
    }
    val args=Seq("--verbosity", "0") ++
      ca.common.tableName.map(x=>Seq("--file-name",x)).getOrElse(Seq())   ++
      (if(ca.common.test) Seq("--test") else Seq()) ++
      ca.common.pioHome.map(x=>Seq("--pio-home",x)).getOrElse(Seq()) ++
      (if(ca.common.verbose) Seq("--verbose") else Seq())

    Runner.runOnSpark(
      "bigdata.analysis.scala.core.TracingCore",
      args,
      ca,
      jarFiles)
  }

}
