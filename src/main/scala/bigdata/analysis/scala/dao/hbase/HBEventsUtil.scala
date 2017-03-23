package bigdata.analysis.scala.dao.hbase




import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.RegexStringComparator
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.hadoop.hbase.filter.QualifierFilter
import org.apache.hadoop.hbase.filter.SkipFilter
import collection.JavaConversions._


import org.json4s.DefaultFormats
import org.json4s.JObject
import org.json4s.native.Serialization.{ read, write }

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import org.apache.commons.codec.binary.Base64
import java.security.MessageDigest

import java.util.UUID
/**
  * Created by frank on 16-12-26.
  */
object HBEventsUtil {
  implicit val formats = DefaultFormats

  // column names for "e" column family
  val colNames: Map[String, Array[Byte]] = Map(
    "event" -> "e",
    "entityType" -> "ety",
    "entityId" -> "eid",
    "targetEntityType" -> "tety",
    "targetEntityId" -> "teid",
    "properties" -> "p",
    "prId" -> "prid",
    "eventTime" -> "et",
    "eventTimeZone" -> "etz",
    "creationTime" -> "ct",
    "creationTimeZone" -> "ctz"
  ).mapValues(Bytes.toBytes(_))

  def hash(logType:String):Array[Byte]={
    val s=logType
    val md5=MessageDigest.getInstance("MD5")
    md5.digest(Bytes.toBytes(s))
  }
  class RowKey(
                val b: Array[Byte]
              ){
    lazy val toBytes: Array[Byte] = b

    override def toString: String = {
      Base64.encodeBase64URLSafeString(toBytes)
    }
  }
  object RowKey{
    def apply(
             logType:String,
             millis:Long,
             uid:Long
             ):RowKey={
      val b=hash(logType) ++
      Bytes.toBytes(millis)  ++Bytes.toBytes(uid)
      new RowKey(b)
    }
    // get RowKey from string representation
    def apply(s: String): RowKey = {
      try {
        apply(Base64.decodeBase64(s))
      } catch {
        case e: Exception => throw new RowKeyException(
          s"Failed to convert String ${s} to RowKey because ${e}", e)
      }
    }

    def apply(b: Array[Byte]): RowKey = {

      new RowKey(b)
    }

  }
  class RowKeyException(val msg: String, val cause: Exception)
    extends Exception(msg, cause) {
    def this(msg: String) = this(msg, null)
  }
  case class PartialRowKey(logType: String,
                           millis: Option[Long] = None) {
    val toBytes: Array[Byte] = {
      hash(logType) ++
        (millis.map(Bytes.toBytes(_)).getOrElse(Array[Byte]()))
    }
  }
  def resultToEvent(result: Result):Map[String,String]={
    val rowKey = RowKey(result.getRow())
    val eBytes = Bytes.toBytes("e")
    val e = result.getFamilyMap(eBytes).toMap
    val resultMap=e.map{kv=>
      (Bytes.toString(kv._1) , Bytes.toString(kv._2))
    }
    resultMap
   /* def getStringCol(col: String): String = {
      val r = result.getValue(eBytes, colNames(col))
      require(r != null,
        s"Failed to get value for column ${col}. " +
          s"Rowkey: ${rowKey.toString} " +
          s"StringBinary: ${Bytes.toStringBinary(result.getRow())}.")

      Bytes.toString(r)
    }
    def getLongCol(col: String): Long = {
      val r = result.getValue(eBytes, colNames(col))
      require(r != null,
        s"Failed to get value for column ${col}. " +
          s"Rowkey: ${rowKey.toString} " +
          s"StringBinary: ${Bytes.toStringBinary(result.getRow())}.")

      Bytes.toLong(r)
    }
    def getOptStringCol(col: String): Option[String] = {
      val r = result.getValue(eBytes, colNames(col))
      if (r == null) {
        None
      } else {
        Some(Bytes.toString(r))
      }
    }

    def getTimestamp(col: String): Long = {
      result.getColumnLatestCell(eBytes, colNames(col)).getTimestamp()
    }
    val event = getStringCol("event")
    val entityType = getStringCol("entityType")
    val entityId = getStringCol("entityId")
    val targetEntityType = getOptStringCol("targetEntityType")
    val targetEntityId = getOptStringCol("targetEntityId")
    val properties: DataMap = getOptStringCol("properties")
      .map(s => DataMap(read[JObject](s))).getOrElse(DataMap())
    val prId = getOptStringCol("prId")
    val eventTimeZone = getOptStringCol("eventTimeZone")
      .map(DateTimeZone.forID(_))
      .getOrElse(EventValidation.defaultTimeZone)
    val eventTime = new DateTime(
      getLongCol("eventTime"), eventTimeZone)
    val creationTimeZone = getOptStringCol("creationTimeZone")
      .map(DateTimeZone.forID(_))
      .getOrElse(EventValidation.defaultTimeZone)
    val creationTime: DateTime = new DateTime(
      getLongCol("creationTime"), creationTimeZone)

    Event(
      eventId = Some(RowKey(result.getRow()).toString),
      event = event,
      entityType = entityType,
      entityId = entityId,
      targetEntityType = targetEntityType,
      targetEntityId = targetEntityId,
      properties = properties,
      eventTime = eventTime,
      tags = Seq(),
      prId = prId,
      creationTime = creationTime
    )*/
  }
  def createScan(
                  logType:String="month",
                  startTime:Option[DateTime]=None,
                  untilTime: Option[DateTime] = None,
                  eventNames: Option[Seq[String]] = None,
                  reversed: Option[Boolean] = None
                ):Scan={
    val scan: Scan = new Scan()
    val minTime: Long = startTime.map(_.getMillis).getOrElse(0)
    val maxTime: Long = untilTime.map(_.getMillis).getOrElse(Long.MaxValue)
    scan.setTimeRange(minTime, maxTime)
    if (reversed.getOrElse(false)) {
      scan.setReversed(true)
    }

    val filters = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    val eBytes = Bytes.toBytes("e")

    // skip the row if the column exists
    def createSkipRowIfColumnExistFilter(col: String): SkipFilter = {
      val comp = new BinaryComparator(colNames(col))
      val q = new QualifierFilter(CompareOp.NOT_EQUAL, comp)
      // filters an entire row if any of the Cell checks do not pass
      new SkipFilter(q)
    }
    if (!filters.getFilters().isEmpty) {
      scan.setFilter(filters)
    }
    scan
  }
}
