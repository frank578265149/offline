package bigdata.analysis.scala.dao.hbase
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
/**
  * Created by frank on 16-12-26.
  */

case class Event (  val eventId: Option[String] = None,
                    val event: String,
                    val entityType: String,
                    val entityId: String,
                    val targetEntityType: Option[String] = None,
                    val targetEntityId: Option[String] = None,
                    val properties: DataMap = DataMap(), // default empty
                    val eventTime: DateTime = DateTime.now,
                    val tags: Seq[String] = Nil,
                    val prId: Option[String] = None,
                    val creationTime: DateTime = DateTime.now
                 ){
  override def toString(): String = {
    s"Event(id=$eventId,event=$event,eType=$entityType,eId=$entityId," +
      s"tType=$targetEntityType,tId=$targetEntityId,p=$properties,t=$eventTime," +
      s"tags=$tags,pKey=$prId,ct=$creationTime)"
  }
}
object EventValidation {
  /** Default time zone is set to UTC */
  val defaultTimeZone = DateTimeZone.UTC

  /** Checks whether an event name contains a reserved prefix
    *
    * @param name Event name
    * @return true if event name starts with \$ or pio_, false otherwise
    */
  def isReservedPrefix(name: String): Boolean = name.startsWith("$") ||
    name.startsWith("pio_")

  /** PredictionIO reserves some single entity event names. They are currently
    * \$set, \$unset, and \$delete.
    */
  val specialEvents = Set("$set", "$unset", "$delete")

  /** Checks whether an event name is a special PredictionIO event name
    *
    * @param name Event name
    * @return true if the name is a special event, false otherwise
    */
  def isSpecialEvents(name: String): Boolean = specialEvents.contains(name)


  /** Defines built-in entity types. The current built-in type is pio_pr. */
  val builtinEntityTypes: Set[String] = Set("pio_pr")

  /** Defines built-in properties. This is currently empty. */
  val builtinProperties: Set[String] = Set()

  /** Checks whether an entity type is a built-in entity type */
  def isBuiltinEntityTypes(name: String): Boolean = builtinEntityTypes.contains(name)


}



