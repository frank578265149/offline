package bigdata.analysis.scala.console

import java.io.File
import java.net.URI

import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.sys.process._
/**
  * Created by frank on 16-10-19.
  */
import scala.sys.process._
/**
  * Created by frank on 16-10-19.
  */
object Runner extends  Logging{
  def envStringToMap(env:String):Map[String,String]={
    env.split(",").flatMap(p=>
      p.split("=") match {
        case Array(k,v)=> List(k->v)
        case _=>Nil
      }
    ).toMap
  }
  def argumentValue(arguments:Seq[String],argumentName:String):Option[String]={
    val argumentIndex=arguments.indexOf(argumentName)
    try{
      arguments(argumentIndex)
      Some(arguments(argumentIndex+1))
    }catch{
      case e:IndexOutOfBoundsException=>None
    }
  }
  def handleScratchFile(
                         fileSystem:Option[FileSystem],
                         uri:Option[URI],
                         localFile:File):String={
    val localFilePath=localFile.getCanonicalPath
    (fileSystem,uri) match {
      case (Some(fs),Some(u)) =>
        val dest=fs.makeQualified(Path.mergePaths(
          new Path(u),
          new Path(localFilePath)
        ))
        println(s"Copying $localFile to ${dest.toString}")
        fs.copyFromLocalFile(new Path(localFilePath),dest)
        dest.toUri.toString
      case _ => localFile.toURI.toString
    }
  }
  def detectFilePaths(
                       fileSystem:Option[FileSystem],
                       uri:Option[URI],
                       args:Seq[String]):Seq[String]={
    args map{ arg=>
      val f=try{
        new File(new URI(arg))
      }catch{
        case e:Throwable => new File(arg)

      }
      if(f.exists()){
        handleScratchFile(fileSystem,uri,f)
      }else{
        arg
      }
    }
  }
  def cleanup(fs:Option[FileSystem],uri:Option[URI]):Unit={
    (fs,uri) match {
      case (Some(f),Some(u)) =>
        f.close()
      case  _ => Unit
    }
  }
  def runOnSpark(
                  className:String,
                  classArgs:Seq[String],
                  ca:ConsoleArgs,
                  extraJars:Seq[URI]
                ):Int={
    val deployMode=
      argumentValue(ca.common.sparkPassThrough,"--deploy-mode").getOrElse("client")
    val master=
      argumentValue(ca.common.sparkPassThrough,"--master").getOrElse("local")
    (ca.common.scratchUri,deployMode,master) match {
      case (Some(u),"client",m) if m!="yarn-cluster" =>
        logger.error("--scrath-uri cannot be set when deploy mode is client")
        return 1
      case (_,"cluster",m) if m.startsWith("spark://") =>
        println("Using cluster deploy mode with Spark standalone cluster is not supported")
        return 1
      case _  => Unit
    }
    val fs=ca.common.scratchUri map { uri=>
      logger.info("Initialize HDFS API for scratch URI")
      FileSystem.get(uri,new Configuration())

    }
    //Collect and serialize PIO_* environment variables
    val pioEnvVars=sys.env.filter(kv=> kv._1.startsWith("PIO_")).map(kv=>
      s"${kv._1}=${kv._2}"
    ).mkString(",")

    val sparkHome=ca.common.sparkHome.getOrElse(
      sys.env.getOrElse("SPARK_HOME",".")
    )
    logger.info(s"SPARK-HOME is:>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>${sparkHome}")
    val mainJar=handleScratchFile(
      fs,
      ca.common.scratchUri,
      Console.coreAssembly(ca.common.pioHome.get)
    )
    //Extra JARS thart are needed by driver
    val driverClassPathPrefix=
      argumentValue(ca.common.sparkPassThrough,"--driver-class-path") map{v=>
        Seq(v)
      }getOrElse {
        Nil
      }
    val extraClasspaths =
      driverClassPathPrefix  ++ WorkflowUtils.thirdPartyClasspaths

    // Extra files that are needed to be passed to --files
    val extraFiles = WorkflowUtils.thirdPartyConfFiles map { f =>
      handleScratchFile(fs, ca.common.scratchUri, new File(f))
    }

    val deployedJars = extraJars map { j =>
      handleScratchFile(fs, ca.common.scratchUri, new File(j))
    }

    val sparkSubmitCommand=
      Seq(Seq(sparkHome,"bin","spark-submit").mkString(File.separator))
    val sparkSubmitJars=if(extraJars.nonEmpty){
      Seq("--jars",deployedJars.map(_.toString).mkString(","))
    }else{
      Nil
    }

    val sparkSubmitFiles = if (extraFiles.nonEmpty) {
      Seq("--files", extraFiles.mkString(","))
    } else {
      Nil
    }

    val sparkSubmitExtraClasspaths = if (extraClasspaths.nonEmpty) {
      Seq("--driver-class-path", extraClasspaths.mkString(":"))
    } else {
      Nil
    }

    val sparkSubmitKryo = if (ca.common.sparkKryo) {
      Seq(
        "--conf",
        "spark.serializer=org.apache.spark.serializer.KryoSerializer")
    } else {
      Nil
    }

    val verbose = if (ca.common.verbose) Seq("--verbose") else Nil
    val sparkSubmit=Seq(
      sparkSubmitCommand,
      ca.common.sparkPassThrough,
      Seq("--class",className),
      sparkSubmitJars,
      sparkSubmitFiles,
      sparkSubmitExtraClasspaths,
      sparkSubmitKryo,
      Seq(mainJar),
      detectFilePaths(fs,ca.common.scratchUri,classArgs),
      Seq("--env",pioEnvVars),
      verbose).flatten.filter(_ !="")
    println(sparkSubmit.toString)
    val proc=Process(
      sparkSubmit,
      None,
      "CLASSPATH" -> "",
      "SPARK_YARN_USER_NEW" -> pioEnvVars).run()
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable{
      def run():Unit={
        cleanup(fs,ca.common.scratchUri)
        proc.destroy()
      }
    }))
    cleanup(fs,ca.common.scratchUri)
    proc.exitValue()
    1
  }

}
