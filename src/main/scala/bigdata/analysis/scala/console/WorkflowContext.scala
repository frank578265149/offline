package bigdata.analysis.scala.console

/**
  * Created by frank on 16-12-7.
  */
import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
object WorkflowContext  extends  Logging{
  def apply(
             batch: String = "",
             executorEnv: Map[String, String] = Map(),
             sparkEnv: Map[String, String] = Map(),
             mode: String = ""
           ): SparkContext = {
    val conf = new SparkConf().setAppName("server").setMaster("local")
    val prefix = if (mode == "") "PredictionIO" else s"PredictionIO ${mode}"
    conf.setAppName(s"${prefix}: ${batch}")
    debug(s"Executor environment received: ${executorEnv}")
    executorEnv.map(kv => conf.setExecutorEnv(kv._1, kv._2))
    debug(s"SparkConf executor environment: ${conf.getExecutorEnv}")
    debug(s"Application environment received: ${sparkEnv}")
    conf.setAll(sparkEnv)
    val sparkConfString = conf.getAll.toSeq
    debug(s"SparkConf environment: $sparkConfString")
    new SparkContext(conf)
  }
  def createHiveContext(sparkContext: SparkContext):HiveContext={
    val hiveContext=new HiveContext(sparkContext)
    hiveContext
  }
}
