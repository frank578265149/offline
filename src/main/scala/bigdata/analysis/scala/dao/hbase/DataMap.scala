package bigdata.analysis.scala.dao.hbase

import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue

import org.json4s._
import org.json4s.native.JsonMethods.parse
/**
  * Created by frank on 16-12-26.
  */
class DataMap (val fields:Map[String,JValue]) extends  Serializable{
  @transient lazy implicit private val formats = DefaultFormats +
    new DateTimeJson4sSupport.Serializer

  /** Converts this DataMap to a List.
    *
    * @return a list of (property name, JSON value) tuples.
    */
  def toList(): List[(String, JValue)] = fields.toList

  /** Converts this DataMap to a JObject.
    *
    * @return the JObject initialized by this DataMap.
    */
  def toJObject(): JObject = JObject(toList())

  /** Converts this DataMap to case class of type T.
    *
    * @return the object of type T.
    */
  def extract[T: Manifest]: T = {
    toJObject().extract[T]
  }
  override
  def toString: String = s"DataMap($fields)"

  override
  def hashCode: Int = 41 + fields.hashCode

  override
  def equals(other: Any): Boolean = other match {
    case that: DataMap => that.canEqual(this) && this.fields.equals(that.fields)
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[DataMap]

}
object  DataMap{
  /** Create an empty DataMap
    * @return an empty DataMap
    */
  def apply(): DataMap = new DataMap(Map[String, JValue]())
  /** Create an DataMap from a Map of String to JValue
    * @param fields a Map of String to JValue
    * @return a new DataMap initialized by fields
    */
  def apply(fields: Map[String, JValue]): DataMap = new DataMap(fields)
  /** Create an DataMap from a JObject
    * @param jObj JObject
    * @return a new DataMap initialized by a JObject
    */
  def apply(jObj: JObject): DataMap = {
    if (jObj == null) {
      apply()
    } else {
      new DataMap(jObj.obj.toMap)
    }
  }

  /** Create an DataMap from a JSON String
    * @param js JSON String. eg """{ "a": 1, "b": "foo" }"""
    * @return a new DataMap initialized by a JSON string
    */
  def apply(js: String): DataMap = apply(parse(js).asInstanceOf[JObject])

}