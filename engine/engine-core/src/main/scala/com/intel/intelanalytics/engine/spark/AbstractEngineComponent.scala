/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.engine.spark

import com.intel.event.EventLogging
import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.engine.plugin.Call
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage, FrameFileStorage }
import com.intel.intelanalytics.engine.spark.graph.{ SparkGraphStorage, HBaseAdminFactory, SparkGraphHBaseBackend }
import com.intel.intelanalytics.engine.spark.model.SparkModelStorage
import com.intel.intelanalytics.engine.spark.partitioners.SparkAutoPartitioner
import com.intel.intelanalytics.engine.spark.threading.EngineExecutionContext
import com.intel.intelanalytics.engine.spark.user.UserStorage
import com.intel.intelanalytics.engine.spark.command._
import com.intel.intelanalytics.repository.{ Profile, SlickMetaStoreComponent, DbProfileComponent }

/**
 * Class Responsible for creating all objects necessary for instantiating an instance of the SparkEngine.
 */
abstract class AbstractEngineComponent(commandLoader: CommandLoader) extends EngineComponent
    with FrameComponent
    with GraphComponent
    with ModelComponent
    with CommandComponent
    with DbProfileComponent
    with SlickMetaStoreComponent
    with EventLogging
    with EventLoggingImplicits {

  implicit lazy val startupCall = Call(null, EngineExecutionContext.global)

  lazy val commandPluginRegistry: CommandPluginRegistry = new CommandPluginRegistry(commandLoader);

  val sparkContextFactory = SparkContextFactory

  val fileStorage = new HdfsFileStorage(EngineConfig.fsRoot)

  val sparkAutoPartitioner = new SparkAutoPartitioner(fileStorage)

  val frameFileStorage = new FrameFileStorage(EngineConfig.fsRoot, fileStorage)

  val frameStorage = new SparkFrameStorage(frameFileStorage, EngineConfig.pageSize, metaStore.asInstanceOf[SlickMetaStore], sparkAutoPartitioner)

  protected val backendGraphStorage: SparkGraphHBaseBackend = new SparkGraphHBaseBackend(hbaseAdminFactory = new HBaseAdminFactory)
  val graphStorage: SparkGraphStorage = new SparkGraphStorage(metaStore, backendGraphStorage, frameStorage)

  val modelStorage: SparkModelStorage = new SparkModelStorage(metaStore.asInstanceOf[SlickMetaStore])

  val userStorage = new UserStorage(metaStore.asInstanceOf[SlickMetaStore])

  val commands = new CommandStorageImpl(metaStore.asInstanceOf[SlickMetaStore])

  lazy val commandExecutor: CommandExecutor = new CommandExecutor(engine, commands)

  override lazy val profile = withContext("engine connecting to metastore") {

    // Initialize a Profile from settings in the config
    val driver = EngineConfig.metaStoreConnectionDriver
    new Profile(Profile.jdbcProfileForDriver(driver),
      connectionString = EngineConfig.metaStoreConnectionUrl,
      driver,
      username = EngineConfig.metaStoreConnectionUsername,
      password = EngineConfig.metaStoreConnectionPassword,
      poolMaxActive = EngineConfig.metaStorePoolMaxActive)
  }(startupCall.eventContext)

  val engine = new EngineImpl(sparkContextFactory,
    commandExecutor, commands, frameStorage, graphStorage, modelStorage, userStorage,
    sparkAutoPartitioner, commandPluginRegistry) {}
}
