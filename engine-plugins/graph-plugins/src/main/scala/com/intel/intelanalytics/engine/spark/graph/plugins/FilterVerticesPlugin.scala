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

package com.intel.intelanalytics.engine.spark.graph.plugins

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.engine.spark.frame._
import com.intel.intelanalytics.domain.schema._
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage, LegacyFrameRdd, PythonRddStorage }
import org.apache.spark.frame.FrameRdd
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.domain.graph.SeamlessGraphMeta
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.domain.command.CommandDoc
import scala.Some
import com.intel.intelanalytics.domain.FilterVerticesArgs
import org.apache.spark.api.python.EnginePythonRdd

//implicit conversion for PairRDD

import org.apache.spark.SparkContext._

import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

class FilterVerticesPlugin extends SparkCommandPlugin[FilterVerticesArgs, FrameEntity] {
  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:vertex/filter"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: FilterVerticesArgs)(implicit invocation: Invocation) = 4

  /**
   * Select all rows which satisfy a predicate
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: FilterVerticesArgs)(implicit invocation: Invocation): FrameEntity = {

    val frames = engine.frames.asInstanceOf[SparkFrameStorage]
    val graphStorage = engine.graphs

    val vertexFrame: SparkFrameData = resolve(arguments.frame)
    require(vertexFrame.meta.isVertexFrame, "vertex frame is required")

    val seamlessGraph: SeamlessGraphMeta = graphStorage.expectSeamless(vertexFrame.meta.graphId.get)
    val filteredRdd = PythonRddStorage.mapWith(vertexFrame.data, arguments.udf, sc = sc)
    filteredRdd.cache()

    val vertexSchema: VertexSchema = vertexFrame.meta.schema.asInstanceOf[VertexSchema]
    FilterVerticesFunctions.removeDanglingEdges(vertexSchema.label, frames, seamlessGraph, sc, filteredRdd)

    val modifiedFrame = frames.saveFrameData(vertexFrame.meta.toReference, filteredRdd)

    filteredRdd.unpersist(blocking = false)

    modifiedFrame
  }
}