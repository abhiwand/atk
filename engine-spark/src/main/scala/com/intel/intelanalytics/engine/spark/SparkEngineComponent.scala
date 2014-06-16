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

import java.util.{ List => JList, ArrayList => JArrayList, Map => JMap }
import com.intel.intelanalytics.engine._
import org.apache.hadoop.fs.{ Path => HPath }
import com.intel.intelanalytics.repository.{Profile, SlickMetaStoreComponent, DbProfileComponent}
import scala.slick.driver.H2Driver
import com.intel.intelanalytics.shared.EventLogging

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import com.intel.intelanalytics.engine.spark.graph.{ SparkGraphStorage, SparkGraphHBaseBackend }
import com.intel.intelanalytics.engine.spark.context.{SparkContextManager, SparkContextFactory}
import com.intel.intelanalytics.engine.spark.frame.SparkFrameStorage
import com.intel.intelanalytics.engine.spark.command.SparkCommandStorage

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
                                    commands.asInstanceOf[CommandStorage], frames, graphs) {}

  override lazy val profile = withContext("engine connecting to metastore") {
    Profile.initializeFromConfig(SparkEngineConfig)
  }

  if (profile.createTables) {
    metaStore.createAllTables()
  }

  val sparkContextManager = new SparkContextManager(SparkEngineConfig.config, new SparkContextFactory)

  val files = new HdfsFileStorage(SparkEngineConfig.fsRoot) {}

  val frames = new SparkFrameStorage(sparkContextManager.context(_),
    SparkEngineConfig.fsRoot, files, SparkEngineConfig.maxRows) {}


  private lazy val admin = new HBaseAdmin(HBaseConfiguration.create())

  val graphs: GraphStorage =
    new SparkGraphStorage(sparkContextManager.context(_),
      metaStore,
      new SparkGraphHBaseBackend(admin), frames)

  val commands = new SparkCommandStorage(metaStore.asInstanceOf[SlickMetaStore]) {}


}

