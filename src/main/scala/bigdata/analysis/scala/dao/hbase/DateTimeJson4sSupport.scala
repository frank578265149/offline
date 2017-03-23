package bigdata.analysis.scala.dao.hbase

import org.json4s.DefaultFormats
import org.json4s._
import org.joda.time.DateTime
import bigdata.analysis.scala.dao.hbase. DataUtils
/**
  * Created by frank on 16-12-26.
  */
object DateTimeJson4sSupport {
  @transient lazy implicit val formats = DefaultFormats

  /** Serialize DateTime to JValue */
  def serializeToJValue: PartialFunction[Any, JValue] = {
    case d: DateTime => JString(DataUtils.dateTimeToString(d))
  }

  /** Deserialize JValue to DateTime */
  def deserializeFromJValue: PartialFunction[JValue, DateTime] = {
    case jv: JValue => DataUtils.stringToDateTime(jv.extract[String])
  }

  /** Custom JSON4S serializer for Joda-Time */
  class Serializer extends CustomSerializer[DateTime](format => (
    deserializeFromJValue, serializeToJValue))


}
