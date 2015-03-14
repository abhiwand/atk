package com.intel.intelanalytics.engine.spark.command

import com.intel.event.EventLogging
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

import scala.collection.mutable

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

  lazy val commandPluginRegistry = new CommandPluginRegistry(new EmptyCommandLoader)

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

  metaStore.initializeSchema()

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

  def usage() = println("Usage: java -cp launcher.jar com.intel.intelanalytics.component.CommandDriver <command_id>")

  def executeCommand(commandId: Long): Unit = {
    val driver = new CommandDriver
    driver.execute(commandId)
  }

  def main(args: Array[String]) = {
    if (args.length < 1) {
      usage()
    }
    else {
      val commandId = args(0).toLong
      executeCommand(commandId)
    }

  }

}
