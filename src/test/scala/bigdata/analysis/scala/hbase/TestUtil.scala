package bigdata.analysis.scala.hbase

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by frank on 17-1-6.
  */
object TestUtil {

  def mapConvert( dataMap:Map[String,String])={
    val rowKey=System.currentTimeMillis()+"frank"
    val family=Bytes.toBytes("e")
    val schema:Seq[String]=dataMap.keys.toSeq
    val p=new Put((Bytes.toBytes(rowKey)))
    for(i <- 0 until schema.size){
      val column= Bytes.toBytes(schema(i))
      val value=Bytes.toBytes(dataMap.getOrElse(schema(i),"30"))
      p.add(family,column,value)
    }

    (new ImmutableBytesWritable,p)
  }
  
}
