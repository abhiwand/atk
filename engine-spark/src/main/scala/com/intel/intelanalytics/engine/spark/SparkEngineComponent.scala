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
import com.intel.intelanalytics.engine.spark.command.{ CommandExecutor, SparkCommandStorage }
import com.intel.intelanalytics.engine.spark.context.{ SparkContextFactory, SparkContextManager }
import com.intel.intelanalytics.engine.spark.frame.SparkFrameStorage
import com.intel.intelanalytics.engine.spark.graph.{ SparkGraphHBaseBackend, SparkGraphStorage }
import com.intel.intelanalytics.engine.spark.queries.{ QueryExecutor, SparkQueryStorage }
import com.intel.intelanalytics.repository.{ DbProfileComponent, Profile, SlickMetaStoreComponent }
import com.intel.intelanalytics.shared.EventLogging
import org.apache.hadoop.fs.{ Path => HPath }
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin

//TODO documentation
//TODO progress notification
//TODO event notification
//TODO pass current user info

class SparkComponent extends EngineComponent
    with FrameComponent
    with CommandComponent
    with FileComponent
    with DbProfileComponent
    with SlickMetaStoreComponent
    with EventLogging {

  lazy val engine = new SparkEngine(sparkContextManager,
    commandExecutor, commands, frames, graphs, queries, queryExecutor, sparkAutoPartitioner) {}

  override lazy val profile = withContext("engine connecting to metastore") {
    Profile.initializeFromConfig(SparkEngineConfig)
  }

  metaStore.initializeSchema()

  val sparkContextManager = new SparkContextManager(SparkEngineConfig.config, new SparkContextFactory)

  val fileStorage = new HdfsFileStorage(SparkEngineConfig.fsRoot)

  val sparkAutoPartitioner = new SparkAutoPartitioner(fileStorage)
  
  val frames = new SparkFrameStorage(sparkContextManager.context(_),
    SparkEngineConfig.fsRoot, files, SparkEngineConfig.pageSize, metaStore.asInstanceOf[SlickMetaStore], sparkAutoPartitioner)

  private lazy val admin = new HBaseAdmin(HBaseConfiguration.create())

  val graphs: GraphStorage =
    new SparkGraphStorage(sparkContextManager.context(_),
      metaStore,
      new SparkGraphHBaseBackend(admin), frames)

  val commands = new SparkCommandStorage(metaStore.asInstanceOf[SlickMetaStore])

  lazy val commandExecutor: CommandExecutor = new CommandExecutor(engine, commands, sparkContextManager)

  val queries = new SparkQueryStorage(metaStore.asInstanceOf[SlickMetaStore], files)

  lazy val queryExecutor: QueryExecutor = new QueryExecutor(engine, queries, sparkContextManager)

}

