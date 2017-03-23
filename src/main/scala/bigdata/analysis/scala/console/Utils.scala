package bigdata.analysis.scala.console

/**
  * Created by frank on 16-10-19.
  */
import org.json4s._
import org.json4s.ext.JodaTimeSerializers
case class EngineManifest(
                           id: String,
                           version: String,
                           name: String,
                           description: Option[String],
                           files: Seq[String],
                           engineFactory: String)
object Utils {
  val json4sDefaultFormats = DefaultFormats.lossless ++ JodaTimeSerializers.all
}
class EngineManifestSerializer
  extends CustomSerializer[EngineManifest](format => (
    {
      case JObject(fields) =>
        val seed = EngineManifest(
          id = "",
          version = "",
          name = "",
          description = None,
          files = Nil,
          engineFactory = "")
        fields.foldLeft(seed) { case (enginemanifest, field) =>
          field match {
            case JField("id", JString(id)) => enginemanifest.copy(id = id)
            case JField("version", JString(version)) =>
              enginemanifest.copy(version = version)
            case JField("name", JString(name)) => enginemanifest.copy(name = name)
            case JField("description", JString(description)) =>
              enginemanifest.copy(description = Some(description))
            case JField("files", JArray(s)) =>
              enginemanifest.copy(files = s.map(t =>
                t match {
                  case JString(file) => file
                  case _ => ""
                }
              ))
            case JField("engineFactory", JString(engineFactory)) =>
              enginemanifest.copy(engineFactory = engineFactory)
            case _ => enginemanifest
          }
        }
    },
    {
      case enginemanifest: EngineManifest =>
        JObject(
          JField("id", JString(enginemanifest.id)) ::
            JField("version", JString(enginemanifest.version)) ::
            JField("name", JString(enginemanifest.name)) ::
            JField("description",
              enginemanifest.description.map(
                x => JString(x)).getOrElse(JNothing)) ::
            JField("files",
              JArray(enginemanifest.files.map(x => JString(x)).toList)) ::
            JField("engineFactory", JString(enginemanifest.engineFactory)) ::
            Nil)
    }
    ))