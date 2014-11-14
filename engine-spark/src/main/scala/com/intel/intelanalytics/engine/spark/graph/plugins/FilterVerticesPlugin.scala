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

package com.intel.intelanalytics.engine.spark.graph.plugins

import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.engine.spark.frame._
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, SparkFrameStorage, LegacyFrameRDD, PythonRDDStorage }
import com.intel.intelanalytics.domain.schema.{ EdgeSchema, VertexSchema, Schema, DataTypes }
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.domain.graph.SeamlessGraphMeta
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.domain.command.CommandDoc
import scala.Some
import com.intel.intelanalytics.domain.FilterVertexRows
import org.apache.spark.api.python.EnginePythonRDD

//implicit conversion for PairRDD

import org.apache.spark.SparkContext._

import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

class FilterVerticesPlugin(graphStorage: SparkGraphStorage) extends SparkCommandPlugin[FilterVertexRows, DataFrame] {
  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:vertex/filter"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = None

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: FilterVertexRows) = 4

  /**
   * Select all rows which satisfy a predicate
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: FilterVertexRows)(implicit invocation: Invocation): DataFrame = {
    val frames = invocation.engine.frames.asInstanceOf[SparkFrameStorage]

    val vertexFrame: SparkFrameData = resolve(arguments.frameId)

    vertexFrame.meta.graphId match {
      case Some(graphId) => {
        val seamlessGraph: SeamlessGraphMeta = graphStorage.expectSeamless(graphId)

        val schema = vertexFrame.meta.schema

        val filteredRdd = PythonRDDStorage.mapWith(vertexFrame.data, arguments.predicate).toLegacyFrameRDD

        val vertexSchema: VertexSchema = schema.asInstanceOf[VertexSchema]
        val updated = FilterVerticesFunctions.removeDanglingEdges(vertexSchema.label, frames, seamlessGraph, sc, filteredRdd)

        updated.vertexMeta(vertexSchema.label)

      }
      case _ => vertexFrame.meta
    }
  }
}

object FilterVerticesFunctions {

  /**
   * Remove dangling edges after filtering on the vertices
   * @param vertexLabel vertex label of the filtered vertex frame
   * @param frameStorage frame storage
   * @param seamlessGraph seemless graph instance
   * @param ctx spark context
   * @param filteredRdd rdd with predicate applied
   */
  def removeDanglingEdges(vertexLabel: String, frameStorage: SparkFrameStorage, seamlessGraph: SeamlessGraphMeta,
                          ctx: SparkContext, filteredRdd: LegacyFrameRDD)(implicit user: UserPrincipal): SeamlessGraphMeta = {
    val vertexFrame = seamlessGraph.vertexMeta(vertexLabel)
    val vertexFrameSchema = vertexFrame.schema

    val sourceRDD: LegacyFrameRDD = frameStorage.loadLegacyFrameRdd(ctx, vertexFrame)
    val droppedVerticesRdd = sourceRDD.map(a => a.toList).subtract(filteredRdd.map(a => a.toList)).map(l => l.toArray)

    val vidColumnIndex = vertexFrameSchema.columnIndex("_vid")
    val droppedVerticesPairRdd = droppedVerticesRdd.map(row => (row(vidColumnIndex), row))

    val newFrameMetas = seamlessGraph.frameMetas.map(frame => {
      val edgeSchema = frame.schema.asInstanceOf[EdgeSchema]
      if (edgeSchema.srcVertexLabel.equals(vertexLabel)) {
        FilterVerticesFunctions.dropDanglingEdgesAndSave(frameStorage, ctx, droppedVerticesPairRdd, frame, "_src_vid")
      }
      else if (edgeSchema.destVertexLabel.equals(vertexLabel)) {
        FilterVerticesFunctions.dropDanglingEdgesAndSave(frameStorage, ctx, droppedVerticesPairRdd, frame, "_dest_vid")
      }
      else {
        frame
      }
    })
    //    seamlessGraph.edgeFrames.foreach(frame => {
    //      val schema = frame.schema.asInstanceOf[EdgeSchema]
    //      if (schema.srcVertexLabel.equals(vertexLabel)) {
    //        FilterVerticesFunctions.dropDanglingEdgesAndSave(frameStorage, ctx, droppedVerticesPairRdd, frame, "_src_vid")
    //      }
    //      if (schema.destVertexLabel.equals(vertexLabel)) {
    //        FilterVerticesFunctions.dropDanglingEdgesAndSave(frameStorage, ctx, droppedVerticesPairRdd, frame, "_dest_vid")
    //      }
    //    })

    seamlessGraph.copy(frameMetas = newFrameMetas)
  }

  /**
   * Remove dangling edges in edge frame and save.
   * @param frameStorage frame storage
   * @param ctx spark context
   * @param droppedVerticesPairRdd pair rdd of dropped vertices. key is the vertex id
   * @param edgeFrame edge frame
   * @param vertexIdColumn source vertex id column or destination id column
   * @return dataframe object
   */
  def dropDanglingEdgesAndSave(frameStorage: SparkFrameStorage,
                               ctx: SparkContext,
                               droppedVerticesPairRdd: RDD[(Any, Row)],
                               edgeFrame: DataFrame,
                               vertexIdColumn: String)(implicit user: UserPrincipal): DataFrame = {
    val edgeRdd = frameStorage.loadLegacyFrameRdd(ctx, edgeFrame)
    val remainingEdges = FilterVerticesFunctions.dropDanglingEdgesFromEdgeRdd(edgeRdd,
      edgeFrame.schema.columnIndex(vertexIdColumn), droppedVerticesPairRdd)
    val toFrameRDD: FrameRDD = new LegacyFrameRDD(edgeFrame.schema, remainingEdges).toFrameRDD()
    frameStorage.saveFrameData(edgeFrame, toFrameRDD, Some(toFrameRDD.count()))
  }

  /**
   * Remove dangling edges in rdd
   * @param edgeRdd edge rdd
   * @param vertexIdColumnIndex column index of vertex id in edge row
   * @param droppedVerticesPairRdd pair rdd of dropped vertices. key is the vertex id
   * @return a edge rdd with dangling edges removed
   */
  def dropDanglingEdgesFromEdgeRdd(edgeRdd: LegacyFrameRDD, vertexIdColumnIndex: Int, droppedVerticesPairRdd: RDD[(Any, Row)]): RDD[Row] = {
    val keyValueEdgeRdd = edgeRdd.map(row => (row(vertexIdColumnIndex), row))
    keyValueEdgeRdd.leftOuterJoin(droppedVerticesPairRdd).filter {
      case (key, (leftResult, rightResult)) => rightResult match {
        case None => true
        case _ => false
      }
    }.map { case (key, (leftResult, rightResult)) => leftResult }
  }
}
