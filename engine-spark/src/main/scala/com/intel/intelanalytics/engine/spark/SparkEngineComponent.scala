//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.engine.spark

import java.util.concurrent.{ Executors, TimeUnit, ScheduledFuture }
import java.util.{ ArrayList => JArrayList, List => JList, Map => JMap }

import com.intel.event.{ EventContext, EventLogging }
import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.engine.gc.GarbageCollector
import com.intel.intelanalytics.engine.plugin.{ Invocation, Call }
import com.intel.intelanalytics.engine.spark.command.{ CommandLoader, CommandPluginRegistry, CommandExecutor, SparkCommandStorage }
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.command.{ CommandExecutor, CommandLoader, CommandPluginRegistry, SparkCommandStorage }
import com.intel.intelanalytics.engine.spark.frame.{ FrameFileStorage, SparkFrameStorage }
import com.intel.intelanalytics.engine.spark.graph.{ HBaseAdminFactory, SparkGraphHBaseBackend, SparkGraphStorage }
import com.intel.intelanalytics.engine.spark.model.SparkModelStorage
import com.intel.intelanalytics.engine.spark.graph.{ SparkGraphHBaseBackend, SparkGraphStorage }
import com.intel.intelanalytics.engine.spark.model.SparkModelStorage
import com.intel.intelanalytics.engine.spark.queries.{ QueryExecutor, SparkQueryStorage }
import com.intel.intelanalytics.engine.spark.user.UserStorage
import com.intel.intelanalytics.engine.spark.util.{ DiskSpaceReporter, JvmVersionReporter }
import com.intel.intelanalytics.repository.{ DbProfileComponent, Profile, SlickMetaStoreComponent }
import com.intel.intelanalytics.security.UserPrincipal
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{ Path => HPath }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.spark.util.{ EnvironmentLogger, JvmVersionReporter, DiskSpaceReporter }
import com.intel.intelanalytics.engine.spark.user.UserStorage

/**
 * Main class for initializing the Spark Engine
 */
class SparkEngineComponent extends EngineComponent
    with FrameComponent
    with GraphComponent
    with ModelComponent
    with CommandComponent
    with DbProfileComponent
    with SlickMetaStoreComponent
    with EventLogging
    with EventLoggingImplicits {

  implicit lazy val startupCall = Call(null)

  EnvironmentLogger.log()

  SparkEngineConfig.logSettings()

  lazy val engine = new SparkEngine(sparkContextFactory,
    commandExecutor, commands, frameStorage, graphStorage, modelStorage, userStorage, queries, queryExecutor,
    sparkAutoPartitioner, new CommandPluginRegistry(new CommandLoader)) {}

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

  lazy val commandExecutor: CommandExecutor = new CommandExecutor(engine, commands, sparkContextFactory)

  val queries = new SparkQueryStorage(metaStore.asInstanceOf[SlickMetaStore], fileStorage)

  lazy val queryExecutor: QueryExecutor = new QueryExecutor(engine, queries, sparkContextFactory)

  JvmVersionReporter.check()

  DiskSpaceReporter.checkDiskSpace()

  GarbageCollector.startup(metaStore, frameFileStorage, backendGraphStorage)

}
