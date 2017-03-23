package bigdata.analysis.scala

/**
  * Created by frank on 17-3-13.
  */
import java.io._
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.sql.functions._
class StringTest extends FunSuite with Matchers with SharedSparkContext  {
  test("producing memberpoints test data"){
   /* val str=createDirectory("/home/frank/IdeaProjects/tracing-source")*/

  }
  def serialize[T](o:T):Array[Byte]={
    val bos=new ByteArrayOutputStream()
    val oos=new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.hashCode()
    bos.toByteArray
  }
  def deserilize[T](bytes:Array[Byte],loader:ClassLoader):T={
    val bis=new ByteArrayInputStream(bytes)
    val ois=new ObjectInputStream(bis){
      override def resolveClass (desc: ObjectStreamClass): Class[_] = {
        Class.forName(desc.getName,false,loader)
      }
    }
    ois.readObject.asInstanceOf[T]
  }
  def createDirectory(root:String,namePrefix:String="spark"):File= {
    var attempts = 0
    val maxAttempts = 3
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp ")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }

      } catch {
        case e: SecurityException => dir = null;
      }

    }
    dir.getCanonicalFile
  }

  /**
    * Execute a command and return the process running the command
    */

  def executeCommand(
                    command:Seq[String],
                    workingDir:File=new File("."),
                    extraEnvironment:Map[String,String]=Map.empty,
                    redirectStderr:Boolean=true
                    ):Process={
    val builder=new ProcessBuilder(command:_*).directory(workingDir)
    val environment=builder.environment()
    for((key,value) <- extraEnvironment){
      environment.put(key,value)
    }
    val process=builder.start()
    if(redirectStderr){
      val threadName="redirect stderr for command " +command(0)
      def log(s:String):Unit=println(s)

    }
    process
  }

}

