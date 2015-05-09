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

class FilterVerticesPlugin(graphStorage: SparkGraphStorage) extends SparkCommandPlugin[FilterVerticesArgs, FrameEntity] {
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

object FilterVerticesFunctions {

  /**
   * Remove dangling edges after filtering on the vertices
   * @param vertexLabel vertex label of the filtered vertex frame
   * @param frameStorage frame storage
   * @param seamlessGraph seamless graph instance
   * @param sc spark context
   * @param filteredRdd rdd with predicate applied
   */
  def removeDanglingEdges(vertexLabel: String, frameStorage: SparkFrameStorage, seamlessGraph: SeamlessGraphMeta,
                          sc: SparkContext, filteredRdd: FrameRdd)(implicit invocation: Invocation): Unit = {
    val vertexFrame = seamlessGraph.vertexMeta(vertexLabel)
    val vertexFrameSchema = vertexFrame.schema

    val originalVertexIds = frameStorage.loadFrameData(sc, vertexFrame).mapRows(_.value(GraphSchema.vidProperty))
    val filteredVertexIds = filteredRdd.mapRows(_.value(GraphSchema.vidProperty))
    val droppedVertexIdsRdd = originalVertexIds.subtract(filteredVertexIds)

    // drop edges connected to the vertices
    seamlessGraph.edgeFrames.map(frame => {
      val edgeSchema = frame.schema.asInstanceOf[EdgeSchema]
      if (edgeSchema.srcVertexLabel.equals(vertexLabel)) {
        FilterVerticesFunctions.dropDanglingEdgesAndSave(frameStorage, sc, droppedVertexIdsRdd, frame, GraphSchema.srcVidProperty)
      }
      else if (edgeSchema.destVertexLabel.equals(vertexLabel)) {
        FilterVerticesFunctions.dropDanglingEdgesAndSave(frameStorage, sc, droppedVertexIdsRdd, frame, GraphSchema.destVidProperty)
      }
    })
  }

  /**
   * Remove dangling edges in edge frame and save.
   * @param frameStorage frame storage
   * @param sc spark context
   * @param droppedVertexIdsRdd rdd of vertex ids
   * @param edgeFrame edge frame
   * @param vertexIdColumn source vertex id column or destination id column
   * @return dataframe object
   */
  def dropDanglingEdgesAndSave(frameStorage: SparkFrameStorage,
                               sc: SparkContext,
                               droppedVertexIdsRdd: RDD[Any],
                               edgeFrame: FrameEntity,
                               vertexIdColumn: String)(implicit invocation: Invocation): FrameEntity = {
    val edgeRdd = frameStorage.loadLegacyFrameRdd(sc, edgeFrame)
    val remainingEdges = FilterVerticesFunctions.dropDanglingEdgesFromEdgeRdd(edgeRdd,
      edgeFrame.schema.columnIndex(vertexIdColumn), droppedVertexIdsRdd)
    val frameRdd = new LegacyFrameRdd(edgeFrame.schema, remainingEdges).toFrameRdd()
    frameStorage.saveFrameData(edgeFrame.toReference, frameRdd)
  }

  /**
   * Remove dangling edges in rdd
   * @param edgeRdd edge rdd
   * @param vertexIdColumnIndex column index of vertex id in edge row
   * @param droppedVertexIdsRdd rdd of vertex ids
   * @return a edge rdd with dangling edges removed
   */
  def dropDanglingEdgesFromEdgeRdd(edgeRdd: LegacyFrameRdd, vertexIdColumnIndex: Int, droppedVertexIdsRdd: RDD[Any]): RDD[Row] = {
    val keyValueEdgeRdd = edgeRdd.map(row => (row(vertexIdColumnIndex), row))
    val droppedVerticesPairRdd = droppedVertexIdsRdd.map(vid => (vid, null))
    keyValueEdgeRdd.leftOuterJoin(droppedVerticesPairRdd).filter {
      case (key, (leftResult, rightResult)) => rightResult match {
        case None => true
        case _ => false
      }
    }.map { case (key, (leftResult, rightResult)) => leftResult }
  }
}