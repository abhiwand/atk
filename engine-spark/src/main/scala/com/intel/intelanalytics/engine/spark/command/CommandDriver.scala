package com.intel.intelanalytics.engine.spark.command

import com.intel.event.adapter.ConsoleEventLog
import com.intel.event.{ EventLogger, EventLogging }
import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.component.Archive
import com.intel.intelanalytics.domain.User
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.engine.plugin.{ Invocation, Call }
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage, FrameFileStorage }
import com.intel.intelanalytics.engine.spark.graph.{ SparkGraphStorage, HBaseAdminFactory, SparkGraphHBaseBackend }
import com.intel.intelanalytics.engine.spark.model.SparkModelStorage
import com.intel.intelanalytics.engine.spark.user.UserStorage
import com.intel.intelanalytics.engine.spark._
import com.intel.intelanalytics.repository.{ Profile, SlickMetaStoreComponent, DbProfileComponent }
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.exception.ExceptionUtils

import scala.collection.mutable
import scala.reflect.io.Directory

class CommandDriver extends EngineComponent
    with FrameComponent
    with GraphComponent
    with ModelComponent
    with CommandComponent
    with DbProfileComponent
    with SlickMetaStoreComponent
    with EventLogging
    with EventLoggingImplicits {

  implicit lazy val startupCall = Call(null)

  lazy val commandPluginRegistry = new CommandPluginRegistry(new PluginCommandLoader)

  val sparkContextFactory = SparkContextFactory

  val fileStorage = new HdfsFileStorage(SparkEngineConfig.fsRoot)

  val sparkAutoPartitioner = new SparkAutoPartitioner(fileStorage)

  val frameFileStorage = new FrameFileStorage(SparkEngineConfig.fsRoot, fileStorage)

  val frameStorage = new SparkFrameStorage(frameFileStorage, SparkEngineConfig.pageSize, metaStore.asInstanceOf[SlickMetaStore], sparkAutoPartitioner)

  private val backendGraphStorage: SparkGraphHBaseBackend = new SparkGraphHBaseBackend(hbaseAdminFactory = new HBaseAdminFactory)
  val graphStorage: SparkGraphStorage = new SparkGraphStorage(metaStore, backendGraphStorage, frameStorage)

  val modelStorage: SparkModelStorage = new SparkModelStorage(metaStore.asInstanceOf[SlickMetaStore])

  val userStorage = new UserStorage(metaStore.asInstanceOf[SlickMetaStore])

  val commands = new SparkCommandStorage(metaStore.asInstanceOf[SlickMetaStore])

  lazy val commandExecutor: CommandExecutor = new CommandExecutor(engine, commands)

  override lazy val profile = withContext("engine connecting to metastore") {

    // Initialize a Profile from settings in the config
    val driver = SparkEngineConfig.metaStoreConnectionDriver
    new Profile(Profile.jdbcProfileForDriver(driver),
      connectionString = SparkEngineConfig.metaStoreConnectionUrl,
      driver,
      username = SparkEngineConfig.metaStoreConnectionUsername,
      password = SparkEngineConfig.metaStoreConnectionPassword)
  }(startupCall.eventContext)

  val engine = new SparkEngine(sparkContextFactory,
    commandExecutor, commands, frameStorage, graphStorage, modelStorage, userStorage, null,
    sparkAutoPartitioner, commandPluginRegistry) {}

  def execute(commandId: Long): Unit = {

    commands.lookup(commandId) match {
      case None => info(s"Command $commandId not found")
      case Some(command) => {
        val user: Option[User] = command.createdById match {
          case Some(id) => metaStore.withSession("se.command.lookup") {
            implicit session =>
              metaStore.userRepo.lookup(id)
          }
          case _ => None
        }
        implicit val invocation: Invocation = new Call(user match {
          case Some(u) => userStorage.createUserPrincipalFromUser(u)
          case _ => null
        })
        commandExecutor.executeCommand(command, commandPluginRegistry)(invocation)
      }
    }
  }
}

object CommandDriver {

  def usage() = println("Usage: java -cp engine-spark.jar com.intel.intelanalytics.component.CommandDriver <command_id>")

  def executeCommand(commandId: Long): Unit = {
    val driver = new CommandDriver
    driver.execute(commandId)
  }

  def main(args: Array[String]) = {
    if (args.length < 1) {
      usage()
    }
    else {
      if (EventLogging.raw) {
        val config = ConfigFactory.load()
        EventLogging.raw = if (config.hasPath("intel.analytics.engine.logging.raw")) config.getBoolean("intel.analytics.engine.logging.raw") else true
      } // else api-server already installed an SLF4j adapter

      println(s"Java Class Path is: ${System.getProperty("java.class.path")}")
      println(s"Current PWD is ${Directory.Current.get.toString()}")
      /* Set to true as for some reason in yarn cluster mode, this doesn't seem to be set on remote driver container */
      try {
        sys.props += Tuple2("SPARK_SUBMIT", "true")
        val commandId = args(0).toLong
        executeCommand(commandId)
      }
      catch {
        case t: Throwable => error(s"Error captured in CommandDriver to prevent percolating up to ApplicationMaster + ${ExceptionUtils.getStackTrace(t)}")
        case _ => error(s"A Non Throwable exception called!!")
      }
      finally {
        sys.props -= "SPARK_SUBMIT"
      }

    }

  }

}
