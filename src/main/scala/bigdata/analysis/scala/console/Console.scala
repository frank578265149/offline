package bigdata.analysis.scala.console

import java.io.File
import java.net.URI

import grizzled.slf4j.Logging
import org.json4s._
import org.json4s.native.Serialization.read

import scala.io.Source

/**
  * Created by frank on 16-10-19.
  */

/**
  * Created by frank on 16-10-19.
  */
case class ConsoleArgs(
                        common:CommonArgs=CommonArgs(),
                        app: AppArgs = AppArgs(),
                        eventServer:EventServerArgs=EventServerArgs(),
                        commands:Seq[String]=Seq()
                      )
case class CommonArgs(
                       pioHome:Option[String]=None,
                       sparkHome:Option[String]=None,
                       sparkPassThrough: Seq[String] = Seq(),
                       driverPassThrough: Seq[String] = Seq(),
                       beginTime:Option[String]=None,
                       endTime:Option[String]=None,
                       verbose:Boolean=false,
                       test:Boolean=false,
                       tableName:Option[String]=None,
                       scratchUri: Option[URI] = None,
                       sparkKryo: Boolean = false,
                       manifestJson:File=new File("engine.josn")
                     )
case class AppArgs(
                    id:Option[Int]=None,
                    name:String="",
                    channel:String="",
                    dataDeleteChannel: Option[String] = None,
                    all: Boolean = false,
                    force: Boolean = false,
                    description: Option[String] = None
                  )
case class EventServerArgs(
                            enabled:Boolean=false,
                            ip:String="0.0.0.0",
                            port:Int=7070,
                            stats:Boolean=false)
object  Console  extends  Logging{
  def main(args: Array[String]) {
    val parser=new scopt.OptionParser[ConsoleArgs]("bigdata") {
      override  def showUsageOnError:Boolean=false
      head("XMHT Command Line Interface Console")
      help("")
      note("")
      opt[String]("pio-home") action { (x, c) =>
        c.copy(common = c.common.copy(pioHome = Some(x)))
      } text("Root directory of a PredictionIO installation.\n" +
        "        Specify this if automatic discovery fail.")
      opt[String]("spark-home") action { (x, c) =>
        c.copy(common = c.common.copy(sparkHome = Some(x)))
      } text("Root directory of an Apache Spark installation.\n" +
        "        If not specified, will try to use the SPARK_HOME\n" +
        "        environmental variable. If this fails as well, default to\n" +
        "        current directory.")
      opt[String]("begin-time") action { (x, c) =>
        c.copy(common = c.common.copy(beginTime = Some(x)))
      } text("Root directory of a XMHT installation.\n" +
        "        Specify this if automatic discovery fail.")
      opt[String]("end-time") action { (x, c) =>
        c.copy(common = c.common.copy(endTime = Some(x)))
      } text("Root directory of an Apache Spark installation.\n" +
        "        If not specified, will try to use the SPARK_HOME\n" +
        "        environmental variable. If this fails as well, default to\n" +
        "        current directory.")
      opt[String]("table-name") action { (x, c) =>
        c.copy(common = c.common.copy(tableName = Some(x)))
      } text("Root directory of an Apache Spark installation.\n" +
        "        If not specified, will try to use the SPARK_HOME\n" +
        "        environmental variable. If this fails as well, default to\n" +
        "        current directory.")
      opt[Unit]("verbose") action { (x, c) =>
        c.copy(common = c.common.copy(verbose = true))
      }
      opt[Unit]("spark-kryo") abbr("sk") action { (x, c) =>
        c.copy(common = c.common.copy(sparkKryo = true))
      }
      opt[Unit]("test")  action { (x, c) =>
        c.copy(common = c.common.copy(test = true))
      }
      opt[String]("scratch-uri") action { (x, c) =>
        c.copy(common = c.common.copy(scratchUri = Some(new URI(x))))
      }
      opt[File]("manifest") abbr("m") action { (x,c)=>
        c.copy(common=c.common.copy(manifestJson=x))
      }
      note("")
      cmd("eventserver").
        text("Launch an Event Server at the specific IP and port.").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "eventserver")
        } children(
        opt[String]("ip") action { (x, c) =>
          c.copy(eventServer = c.eventServer.copy(ip = x))
        },
        opt[Int]("port") action { (x, c) =>
          c.copy(eventServer = c.eventServer.copy(port = x))
        } text("Port to bind to. Default: 7070"),
        opt[Unit]("stats") action { (x, c) =>
          c.copy(eventServer = c.eventServer.copy(stats = true))
        }
        )
      note("")
      cmd("run").
        text("Launch an Event Server at the specific IP and port.").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "run")
        } children(
        opt[String]("ip") action { (x, c) =>
          c.copy(eventServer = c.eventServer.copy(ip = x))
        },
        opt[Int]("port") action { (x, c) =>
          c.copy(eventServer = c.eventServer.copy(port = x))
        } text("Port to bind to. Default: 7070"),
        opt[Unit]("stats") action { (x, c) =>
          c.copy(eventServer = c.eventServer.copy(stats = true))
        }
        )
      note("")
      cmd("app").
        text("Manage apps.\n").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "app")
        } children(
        cmd("join").
          text("Create a new app key to app ID mapping.").
          action { (_, c) =>
            c.copy(commands = c.commands :+ "join")
          } children(
          opt[Int]("id") action { (x, c) =>
            c.copy(app = c.app.copy(id = Some(x)))
          },
          opt[String]("description") action { (x, c) =>
            c.copy(app = c.app.copy(description = Some(x)))
          },
          arg[String]("<name>") action { (x, c) =>
            c.copy(app = c.app.copy(name = x))
          }
          ),
        note(""),
        cmd("list").
          text("List all apps.").
          action { (_, c) =>
            c.copy(commands = c.commands :+ "list")
          },
        note(""),
        cmd("show").
          text("Show details of an app.").
          action { (_, c) =>
            c.copy(commands = c.commands :+ "show")
          } children (
          arg[String]("<name>") action { (x, c) =>
            c.copy(app = c.app.copy(name = x))
          } text("Name of the app to be shown.")
          ),
        note(""),
        cmd("delete").
          text("Delete an app.").
          action { (_, c) =>
            c.copy(commands = c.commands :+ "delete")
          } children(
          arg[String]("<name>") action { (x, c) =>
            c.copy(app = c.app.copy(name = x))
          } text("Name of the app to be deleted."),
          opt[Unit]("force") abbr("f") action { (x, c) =>
            c.copy(app = c.app.copy(force = true))
          } text("Delete an app without prompting for confirmation")
          ),
        note(""),
        cmd("data-delete").
          text("Delete data of an app").
          action { (_, c) =>
            c.copy(commands = c.commands :+ "data-delete")
          } children(
          arg[String]("<name>") action { (x, c) =>
            c.copy(app = c.app.copy(name = x))
          } text("Name of the app whose data to be deleted."),
          opt[String]("channel") action { (x, c) =>
            c.copy(app = c.app.copy(dataDeleteChannel = Some(x)))
          } text("Name of channel whose data to be deleted."),
          opt[Unit]("all") action { (x, c) =>
            c.copy(app = c.app.copy(all = true))
          } text("Delete data of all channels including default"),
          opt[Unit]("force") abbr("f") action { (x, c) =>
            c.copy(app = c.app.copy(force = true))
          } text("Delete data of an app without prompting for confirmation")
          ),
        note(""),
        cmd("channel-new").
          text("Create a new channel for the app.").
          action { (_, c) =>
            c.copy(commands = c.commands :+ "channel-new")
          } children (
          arg[String]("<name>") action { (x, c) =>
            c.copy(app = c.app.copy(name = x))
          } text("App name."),
          arg[String]("<channel>") action { (x, c) =>
            c.copy(app = c.app.copy(channel = x))
          } text ("Channel name to be created.")
          ),
        note(""),
        cmd("channel-delete").
          text("Delete a channel of the app.").
          action { (_, c) =>
            c.copy(commands = c.commands :+ "channel-delete")
          } children (
          arg[String]("<name>") action { (x, c) =>
            c.copy(app = c.app.copy(name = x))
          } text("App name."),
          arg[String]("<channel>") action { (x, c) =>
            c.copy(app = c.app.copy(channel = x))
          } text ("Channel name to be deleted."),
          opt[Unit]("force") abbr("f") action { (x, c) =>
            c.copy(app = c.app.copy(force = true))
          } text("Delete a channel of the app without prompting for confirmation")
          )
        )
    }
    val separatorIndex = args.indexWhere(_ == "--")
    val (consoleArgs, theRest) =
      if (separatorIndex == -1) {
        (args, Array[String]())
      } else {
        args.splitAt(separatorIndex)
      }
    val allPassThroughArgs = theRest.drop(1)
    val secondSepIdx = allPassThroughArgs.indexWhere(_ == "--")
    val (sparkPassThroughArgs, driverPassThroughArgs) =
      if (secondSepIdx == -1) {
        (allPassThroughArgs, Array[String]())
      } else {
        val t = allPassThroughArgs.splitAt(secondSepIdx)
        (t._1, t._2.drop(1))
      }
    parser.parse(consoleArgs, ConsoleArgs()) map { pca =>
      val ca = pca.copy(common = pca.common.copy(
        sparkPassThrough = sparkPassThroughArgs,
        driverPassThrough = driverPassThroughArgs))
      val rv: Int = ca.commands match {
        case Seq("") =>
          System.err.println(help())
          1
        case Seq("version") =>
          version(ca)
          0
        case Seq("eventserver") =>
          eventServer(ca)
          1
        case Seq("run") =>
          run(ca)
          1
        case Seq("app", "join") =>
          App.runJoin(ca)
        case Seq("app", "list") =>
          App.runJoin(ca)
        case Seq("app", "show") =>
          App.runJoin(ca)
        case Seq("app", "delete") =>
          App.runJoin(ca)
        case Seq("app", "data-delete") =>
          App.runJoin(ca)
        case Seq("app", "channel-new") =>
          App.runJoin(ca)
        case _ =>
          System.err.println(help(ca.commands))
          1
      }
      sys.exit(rv)
    } getOrElse {
      val command = args.toSeq.filterNot(_.startsWith("--")).head
      System.err.println(help(Seq(command)))
      sys.exit(1)
    }
  }
  def help(commands:Seq[String]=Seq()):String={
    if(commands.isEmpty){
      mainHelp
    }else{
      val stripped=
        (if (commands.head == "help") commands.drop(1) else commands).
          mkString("-")
      helpText.getOrElse(stripped, s"Help is unavailable for ${stripped}.")
    }
  }
  val mainHelp="need help"
  val helpText=Map("run"-> "help")
  def version(ca:ConsoleArgs):Unit=println("version info ")
  def run(ca:ConsoleArgs):Unit={
    logger.info(s"Create server process event from kafka to mysql")
    val em=readManifestJson(ca.common.manifestJson)
    RunAnalysis.newRunMysqlWorkflow(ca,em)
  }
  def coreAssembly(pioHome:String):File={
    val core=s"tracing-source-1.0-SNAPSHOT-jar-with-dependencies.jar"
    val coreDir=
      if(new File(pioHome+File.separator+"RELEASE").exists()){
        new File(pioHome +File.separator+"lib")
      }else{
        new   File(pioHome+File.separator+"target")
      }
    val coreFile=new File(coreDir,core)
    if(coreFile.exists()){
      coreFile
    }else{
      logger.error(s"core assembly (${coreFile.getCanonicalPath})does not existing Aborting")
      sys.exit(1)
    }
  }
  def eventServer(ca:ConsoleArgs):Unit={
    logger.info(s"Server starting >>>>>>>>>>>>>>>>>>>>. ")
    val em=readManifestJson(ca.common.manifestJson)
    RunAnalysis.newRunWorkflow(ca,em)

  }
  def readManifestJson(json:File):EngineManifest={
    implicit  val formats=Utils.json4sDefaultFormats +
      new EngineManifestSerializer
    try{
      read[EngineManifest](Source.fromFile(json).mkString)
    }catch{
      case e:java.io.FileNotFoundException=>
        println(s"${json.getCanonicalPath} does not exist content")
        sys.exit(1)
      case e:MappingException=>
        println(s"${json.getCanonicalPath} has invalid content: " +
          e.getMessage)
        sys.exit(1)
    }
  }

}
