//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

import java.util.{ ArrayList => JArrayList, List => JList, Map => JMap }

import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.engine.spark.command.{ CommandLoader, CommandPluginRegistry, CommandExecutor, SparkCommandStorage }
import com.intel.intelanalytics.engine.spark.context.{ SparkContextFactory, SparkContextManager }
import com.intel.intelanalytics.engine.spark.frame.{ FrameFileStorage, SparkFrameStorage }
import com.intel.intelanalytics.engine.spark.graph.{ SparkGraphHBaseBackend, SparkGraphStorage }
import com.intel.intelanalytics.engine.spark.queries.{ QueryExecutor, SparkQueryStorage }
import com.intel.intelanalytics.repository.{ DbProfileComponent, Profile, SlickMetaStoreComponent }
import com.intel.intelanalytics.shared.EventLogging
import org.apache.hadoop.fs.{ Path => HPath }
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import com.intel.intelanalytics.security.UserPrincipal

//TODO documentation
//TODO progress notification
//TODO event notification
//TODO pass current user info

class SparkComponent extends EngineComponent
    with FrameComponent
    with GraphComponent
    with CommandComponent
    with DbProfileComponent
    with SlickMetaStoreComponent
    with EventLogging {

  SparkEngineConfig.logSettings()

  lazy val engine = new SparkEngine(sparkContextManager,
    commandExecutor, commands, frames, graphs, queries, queryExecutor, sparkAutoPartitioner, new CommandPluginRegistry(new CommandLoader)) {}

  override lazy val profile = withContext("engine connecting to metastore") {
    Profile.initializeFromConfig(SparkEngineConfig)
  }

  metaStore.initializeSchema()

  val sparkContextManager = new SparkContextManager(SparkEngineConfig.config, new SparkContextFactory)

  val fileStorage = new HdfsFileStorage(SparkEngineConfig.fsRoot)

  val sparkAutoPartitioner = new SparkAutoPartitioner(fileStorage)

  val frameFileStorage = new FrameFileStorage(SparkEngineConfig.fsRoot, fileStorage)

  val getContextFunc = (user: UserPrincipal) => sparkContextManager.context(user, "query")
  val frames = new SparkFrameStorage(frameFileStorage, SparkEngineConfig.pageSize, metaStore.asInstanceOf[SlickMetaStore], sparkAutoPartitioner, getContextFunc)

  private lazy val admin = new HBaseAdmin(HBaseConfiguration.create())

  val graphs: GraphStorage =
    new SparkGraphStorage(metaStore,
      new SparkGraphHBaseBackend(admin), frames)

  val commands = new SparkCommandStorage(metaStore.asInstanceOf[SlickMetaStore])

  lazy val commandExecutor: CommandExecutor = new CommandExecutor(engine, commands, sparkContextManager)

  val queries = new SparkQueryStorage(metaStore.asInstanceOf[SlickMetaStore], fileStorage)

  lazy val queryExecutor: QueryExecutor = new QueryExecutor(engine, queries, sparkContextManager)

}

