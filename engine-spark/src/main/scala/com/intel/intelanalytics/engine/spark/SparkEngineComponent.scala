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

import org.apache.spark.api.python._
import java.util.{ List => JList, ArrayList => JArrayList, Map => JMap }
import org.apache.spark.broadcast.Broadcast
import com.intel.intelanalytics.domain._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine._
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import java.nio.file.Paths
import java.io._
import com.intel.intelanalytics.engine.Rows._
import java.util.concurrent.atomic.AtomicLong
import org.apache.hadoop.fs.{ LocalFileSystem, FileSystem }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DistributedFileSystem
import java.nio.file.Path
import scala.io.Codec
import scala.io.{ Codec, Source }
import org.apache.hadoop.fs.{ Path => HPath }

import scala.util.matching.Regex
import com.intel.event.EventContext
import com.intel.intelanalytics.repository.{ SlickMetaStoreComponent, DbProfileComponent }
import scala.slick.driver.H2Driver
import scala.util.Try
import com.typesafe.config.ConfigFactory
import com.intel.intelanalytics.shared.EventLogging
import com.intel.graphbuilder.driver.spark.titan.examples.ExamplesUtils
import com.thinkaurelius.titan.core.{ TitanGraph, TitanFactory }
import com.tinkerpop.blueprints.{ Direction, Vertex }
import com.intel.graphbuilder.util.SerializableBaseConfiguration

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin

import scala.util.Failure
import scala.Some
import scala.collection.JavaConverters._
import com.intel.intelanalytics.security.UserPrincipal
import scala.util.Success
import com.intel.intelanalytics.domain.Partial
import com.intel.intelanalytics.domain.SeparatorArgs
import com.intel.intelanalytics.domain.Error
import com.intel.intelanalytics.engine.spark.graph.{ SparkGraphStorage, SparkGraphHBaseBackend }

import spray.json._
import com.intel.intelanalytics.domain.schema.DataTypes
import DataTypes.DataType
import com.intel.intelanalytics.engine.spark.context.{SparkContextFactory, Context}
import com.intel.intelanalytics.engine.spark.frame.{RDDJoinParam, RowParser}
import com.intel.intelanalytics.domain.command._
import com.intel.intelanalytics.domain.frame.{DataFrame, DataFrameTemplate}
import com.intel.intelanalytics.domain.graph.{GraphTemplate, Graph}

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

  import DomainJsonProtocol._
  lazy val configuration: SparkEngineConfiguration = new SparkEngineConfiguration()

  val engine = new SparkEngine {}

  //TODO: choose database profile driver class from config
  override lazy val profile = withContext("engine connecting to metastore") {
    new Profile(H2Driver, connectionString = configuration.connectionString, driver = configuration.driver)
  }

  lazy val fsRoot = configuration.fsRoot

  val sparkContextManager = new SparkContextManager(configuration.config, new SparkContextFactory)

  //TODO: only create if the datatabase doesn't already exist. So far this is in-memory only,
  //but when we want to use postgresql or mysql or something, we won't usually be creating tables here.
  metaStore.create()


  val files = new HdfsFileStorage {}


  val frames = new SparkFrameStorage {}


  val graphs: GraphStorage =
    new SparkGraphStorage(engine.context(_),
      metaStore,
      new SparkGraphHBaseBackend(new HBaseAdmin(HBaseConfiguration.create())),
      frames)

  val commands = new SparkCommandStorage {}


}

