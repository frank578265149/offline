package bigdata.analysis.scala.console


/*

import java.io.PrintWriter
import java.io.Serializable
import java.io.StringWriter
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.event.Logging
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import bigdata.analysis.scala.common.authentication.KeyAuthentication
import bigdata.analysis.scala.common.configuration.SSLConfiguration
import bigdata.analysis.scala.console.JsonExtractorOption.JsonExtractorOption
import com.github.nscala_time.time.Imports.DateTime
import com.twitter.chill.{KryoBase, KryoInstantiator, ScalaKryoInstantiator}
import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.write
/*import spray.can.Http
import spray.can.server.ServerSettings
import spray.http.MediaTypes._
import spray.http._
import spray.httpx.Json4sSupport
import spray.routing._
import spray.routing.authentication.{BasicAuth, UserPass}*/
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.write

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.future
import scala.language.existentials
import scala.util.Failure
import scala.util.Random
import scala.util.Success
import scalaj.http.HttpOptions
/**
  * Created by frank on 16-12-1.
  */

case class ServerConfig(
                       batch:String="",
                       env:Option[String]=None,
                       ip:String="0.0.0.0",
                       port:Int=8000,
                       feedback:Boolean=false,
                       accessKey: Option[String] = None,
                       logUrl: Option[String] = None,
                       logPrefix: Option[String] = None,
                       logFile: Option[String] = None,
                       verbose: Boolean = false,
                       debug: Boolean = false,
                       jsonExtractor: JsonExtractorOption = JsonExtractorOption.Both)

case class StartServer()
case class BindServer()
case class StopServer()
case class ReloadServer()


object CreateServer  extends  Logging{
  val actorSystem=ActorSystem("xmht-server")

  def main (args: Array[String]) {
    val parser = new scopt.OptionParser[ServerConfig]("CreateServer") {
      opt[String]("batch") action { (x, c) =>
        c.copy(batch = x)
      } text("Batch label of the deployment.")
      opt[String]("ip") action { (x, c) =>
        c.copy(ip = x)
      }
      opt[String]("env") action { (x, c) =>
        c.copy(env = Some(x))
      } text("Comma-separated list of environmental variables (in 'FOO=BAR' " +
        "format) to pass to the Spark execution environment.")
      opt[Int]("port") action { (x, c) =>
        c.copy(port = x)
      } text("Port to bind to (default: 8000).")
      opt[Unit]("feedback") action { (_, c) =>
        c.copy(feedback = true)
      } text("Enable feedback loop to event server.")
      opt[String]("accesskey") action { (x, c) =>
        c.copy(accessKey = Some(x))
      } text("Event server access key.")
      opt[String]("log-url") action { (x, c) =>
        c.copy(logUrl = Some(x))
      }
      opt[String]("log-prefix") action { (x, c) =>
        c.copy(logPrefix = Some(x))
      }
      opt[String]("log-file") action { (x, c) =>
        c.copy(logFile = Some(x))
      }
      opt[Unit]("verbose") action { (x, c) =>
        c.copy(verbose = true)
      } text("Enable verbose output.")
      opt[Unit]("debug") action { (x, c) =>
        c.copy(debug = true)
      } text("Enable debug output.")
    }
    parser.parse(args, ServerConfig()) map { sc =>
      val master=actorSystem.actorOf(Props(
       classOf[MasterActor],
        sc),
        "master")
      implicit  val timeout=Timeout(5.seconds)
      master ? StartServer()
      actorSystem.awaitTermination()
    }
  }
  def createServerActorWithEngine(
                                 sc:ServerConfig
                                 ):ActorRef={
    val sparkContext=WorkflowContext(
      batch="batch",
      mode="Serving"
    )
    val hiveContext=WorkflowContext.createHiveContext(sparkContext)
    val engine=new Engine(sparkContext,hiveContext)
    actorSystem.actorOf(
      Props(
        classOf[ServerActor],
        engine,
        sparkContext
     ))
  }
}
class  MasterActor(
                  sc:ServerConfig
                  ) extends  Actor with SSLConfiguration with KeyAuthentication {

  val log=Logging(context.system,this)
  implicit  val system=context.system
  var sprayHttpListener: Option[ActorRef] = None
  var currentServerActor: Option[ActorRef] = None
  var retry = 3
  val serverConfig = ConfigFactory.load("server.conf")
  val sslEnforced = serverConfig.getBoolean("org.apache.predictionio.server.ssl-enforced")
  val protocol = if (sslEnforced) "https://" else "http://"
  def receive:Actor.Receive={
    case x:StartServer=>
      val actor=createServerActor(
        sc
      )
      currentServerActor=Some(actor)
      self ! BindServer()
    case x:BindServer=>
      currentServerActor map{actor=>
        val settings=ServerSettings(system)
        IO(Http) ! Http.Bind(
          actor,
          interface = sc.ip,
          port = sc.port,
          settings = Some(settings.copy(sslEncryption = sslEnforced)))
      } getOrElse{
        log.error("Cantnot bind a non-existing server backend")
      }
    case x: Http.Bound =>
      val serverUrl = s"${protocol}${sc.ip}:${sc.port}"
      log.info(s"Engine is deployed and running. Engine API is live at ${serverUrl}.")
      sprayHttpListener = Some(sender)
    case x: Http.CommandFailed =>
      if (retry > 0) {
        retry -= 1
        log.error(s"Bind failed. Retrying... ($retry more trial(s))")
        context.system.scheduler.scheduleOnce(1.seconds) {
          self ! BindServer()
        }
      } else {
        log.error("Bind failed. Shutting down.")
        system.shutdown
      }

  }
  def createServerActor(
                       sc:ServerConfig
                       ):ActorRef={
    CreateServer.createServerActorWithEngine(sc)
  }
}
class ServerActor(
                   val args: ServerConfig,
                   val engine: Engine
                 ) extends Actor with HttpService with KeyAuthentication {
  val serverStartTime = DateTime.now
  val log = Logging(context.system, this)

  var requestCount: Int = 0
  var avgServingSec: Double = 0.0
  var lastServingSec: Double = 0.0

  /** The following is required by HttpService */
  def actorRefFactory: ActorContext = context

  implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  def receive: Actor.Receive = runRoute(myRoute)

  val feedbackEnabled = if (args.feedback) {
    if (args.accessKey.isEmpty) {
      log.error("Feedback loop cannot be enabled because accessKey is empty.")
      false
    } else {
      true
    }
  } else false


  def getStackTraceString(e: Throwable): String = {
    val writer = new StringWriter()
    val printWriter = new PrintWriter(writer)
    e.printStackTrace(printWriter)
    writer.toString
  }

  val myRoute =
    path("") {
      get {
        respondWithMediaType(`text/html`) {
          detach() {
            complete {
             s"hello"

            }
          }
        }
      }
    } ~
      path("queries.json") {
        post {
          detach(){
            entity(as[String]){queryString=>
              try{
                val servingStartTime = DateTime.now
                val jsonExtractorOption = args.jsonExtractor
                val queryTime = DateTime.now
                //Extract Query from json
                val query=JsonExtractor.extract(jsonExtractorOption,
                  queryString,
                  classOf[Query]
                )
                val dt=engine.searchDetails(query.command)
                complete{
                  s"${query.name}:${query.command}:----->${dt}"
                }
              }catch{
                case e:MappingException=>
                  log.error(
                    s"Query '$queryString' is invalid .Reason :${e.getMessage}"
                  )
                  args.logUrl map {url=>
                    "send error log "
                  }
                  complete(StatusCodes.BadRequest, e.getMessage)
                case e:Throwable=>
                  val msg = s"Query:\n$queryString\n\nStack Trace:\n" +
                    s"${getStackTraceString(e)}\n\n"
                  log.error(msg)
                  args.logUrl map { url =>
                    "String "
                  }
                  complete(StatusCodes.InternalServerError, msg)

              }


            }
          }

        }
      } ~
      path("reload") {
          post {
            complete {
              "Reloading..."
            }
        }
      }~
  path("search" ){
    parameter('key.as[String]){key=>
      get{
        complete{
          s"segmen${key}"
        }
      }
    }

  }


}
case class  Query(
 val name:String="",
 val command:String=""

)*/
