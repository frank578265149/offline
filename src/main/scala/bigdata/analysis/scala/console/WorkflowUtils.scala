package bigdata.analysis.scala.console

import java.io.File

import grizzled.slf4j.Logging
import org.apache.log4j.{Level, LogManager}
/**
  * Created by frank on 16-10-19.
  */
object WorkflowUtils  extends  Logging{
   /** Grab environmental variables thar start with  'PIO_'.*/
  def pioEnvVars:Map[String,String]=
     sys.env.filter(kv => kv._1.startsWith("PIO_"))

  /**
    * Detect third party software configuration files to be submitted as
    * extras  to Apache spark this make sure all executors receive the sanme
    * configuration
    */
  def thirdPartyConfFiles:Seq[String]={
    val thirdPartyFiles=Map(
      "PIO_CONF_DIR" -> "log4j.properties",
      "ES_CONF_DIR" -> "elasticsearch.yml",
      "HADOOP_CONF_DIR" -> "core-site.xml",
      "HIVE_CONF_DIR"->"hive-site.xml",
      "HBASE_CONF_DIR" -> "hbase-site.xml")

    thirdPartyFiles.keys.toSeq.map{k:String=>
      sys.env.get(k) map{ x=>
        val p=Seq(x,thirdPartyFiles(k)).mkString(File.separator)
        if (new File(p).exists) Seq(p) else Seq[String]()
      } getOrElse Seq[String]()
    }.flatten
  }
  def thirdPartyClasspaths: Seq[String] = {
    val thirdPartyPaths = Seq(
      "PIO_CONF_DIR",
      "ES_CONF_DIR",
      "HIVE_CONF_DIR",
      "POSTGRES_JDBC_DRIVER",
      "MYSQL_JDBC_DRIVER",
      "HADOOP_CONF_DIR",
      "HBASE_CONF_DIR")
    thirdPartyPaths.map(p =>
      sys.env.get(p).map(Seq(_)).getOrElse(Seq[String]())
    ).flatten
  }
  def modifyLogging(verbose: Boolean): Unit = {
    val rootLoggerLevel = if (verbose) Level.TRACE else Level.INFO
    val chattyLoggerLevel = if (verbose) Level.INFO else Level.WARN

    LogManager.getRootLogger.setLevel(rootLoggerLevel)

    LogManager.getLogger("org.elasticsearch").setLevel(chattyLoggerLevel)
    LogManager.getLogger("org.apache.hadoop").setLevel(chattyLoggerLevel)
    LogManager.getLogger("org.apache.spark").setLevel(chattyLoggerLevel)
    LogManager.getLogger("org.eclipse.jetty").setLevel(chattyLoggerLevel)
    LogManager.getLogger("akka").setLevel(chattyLoggerLevel)
  }
}
