package com.intel.intelanalytics.engine.spark

import com.intel.event.EventLogging
import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.engine.plugin.Call
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage, FrameFileStorage }
import com.intel.intelanalytics.engine.spark.graph.{ SparkGraphStorage, HBaseAdminFactory, SparkGraphHBaseBackend }
import com.intel.intelanalytics.engine.spark.model.SparkModelStorage
import com.intel.intelanalytics.engine.spark.queries.SparkQueryStorage
import com.intel.intelanalytics.engine.spark.user.UserStorage
import com.intel.intelanalytics.engine.spark.command.{ CommandExecutor, SparkCommandStorage, CommandPluginRegistry, CommandLoaderTrait }
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.repository.{ Profile, SlickMetaStoreComponent, DbProfileComponent }

/**
 * Class Responsible for creating all objects necessary for instantiating an instance of the SparkEngine.
 */
abstract class AbstractEngineComponent(commandLoader: CommandLoaderTrait) extends EngineComponent
    with FrameComponent
    with GraphComponent
    with ModelComponent
    with CommandComponent
    with DbProfileComponent
    with SlickMetaStoreComponent
    with EventLogging
    with EventLoggingImplicits {

  implicit lazy val startupCall = Call(null)

  lazy val commandPluginRegistry: CommandPluginRegistry = new CommandPluginRegistry(commandLoader);

  val sparkContextFactory = SparkContextFactory

  val fileStorage = new HdfsFileStorage(SparkEngineConfig.fsRoot)

  val sparkAutoPartitioner = new SparkAutoPartitioner(fileStorage)

  val frameFileStorage = new FrameFileStorage(SparkEngineConfig.fsRoot, fileStorage)

  val frameStorage = new SparkFrameStorage(frameFileStorage, SparkEngineConfig.pageSize, metaStore.asInstanceOf[SlickMetaStore], sparkAutoPartitioner)

  protected val backendGraphStorage: SparkGraphHBaseBackend = new SparkGraphHBaseBackend(hbaseAdminFactory = new HBaseAdminFactory)
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

  val queries = new SparkQueryStorage(metaStore.asInstanceOf[SlickMetaStore], fileStorage)

  val engine = new SparkEngine(sparkContextFactory,
    commandExecutor, commands, frameStorage, graphStorage, modelStorage, userStorage, queries,
    sparkAutoPartitioner, commandPluginRegistry) {}
}