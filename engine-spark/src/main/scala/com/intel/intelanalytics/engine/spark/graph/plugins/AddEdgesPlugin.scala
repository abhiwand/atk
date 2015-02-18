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
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.graph.construction.{ AddEdgesArgs, AddVerticesArgs }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.plugin.{ CommandInvocation, Invocation }
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage, FrameRDD }
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import com.intel.intelanalytics.domain.schema.{ EdgeSchema, DataTypes }
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, RowWrapper }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.SparkContext._

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Add Vertices to a Vertex Frame
 */
class AddEdgesPlugin(addVerticesPlugin: AddVerticesPlugin) extends SparkCommandPlugin[AddEdgesArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graph/sampling/vertex_sample
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:edge/add_edges"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: AddEdgesArgs)(implicit invocation: Invocation): Int = {
    if (arguments.isCreateMissingVertices) {
      15
    }
    else {
      8
    }
  }

  /**
   * Add Vertices to a Seamless Graph
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: AddEdgesArgs)(implicit invocation: Invocation): UnitReturn = {
    // dependencies (later to be replaced with dependency injection)
    val graphs = engine.graphs.asInstanceOf[SparkGraphStorage]
    val frames = engine.frames.asInstanceOf[SparkFrameStorage]
    // validate arguments
    val edgeFrameEntity = frames.expectFrame(arguments.edgeFrame)
    require(edgeFrameEntity.isEdgeFrame, "add edges requires an edge frame")
    var graph = graphs.expectSeamless(edgeFrameEntity.graphId.get)
    val sourceFrameMeta = frames.expectFrame(arguments.sourceFrame)
    sourceFrameMeta.schema.validateColumnsExist(arguments.allColumnNames)

    // run the operation

    // load source data
    val sourceRdd = frames.loadFrameData(sc, sourceFrameMeta)

    // assign unique ids to source data
    val edgeDataToAdd = sourceRdd.selectColumns(arguments.allColumnNames).assignUniqueIds("_eid", startId = graph.nextId())
    edgeDataToAdd.cache()
    graphs.updateIdCounter(graph.id, edgeDataToAdd.count())

    // convert to appropriate schema, adding edge system columns
    val edgesWithoutVids = edgeDataToAdd.convertToNewSchema(edgeDataToAdd.frameSchema.addColumn("_src_vid", DataTypes.int64).addColumn("_dest_vid", DataTypes.int64))
    edgesWithoutVids.cache()
    edgeDataToAdd.unpersist(blocking = false)

    val srcLabel = edgeFrameEntity.schema.asInstanceOf[EdgeSchema].srcVertexLabel
    val destLabel = edgeFrameEntity.schema.asInstanceOf[EdgeSchema].destVertexLabel

    // create vertices from edge data and append to vertex frames
    if (arguments.isCreateMissingVertices) {
      val sourceVertexData = edgesWithoutVids.selectColumns(List(arguments.columnNameForSourceVertexId))
      val destVertexData = edgesWithoutVids.selectColumns(List(arguments.columnNameForDestVertexId))
      addVerticesPlugin.addVertices(sc, AddVerticesArgs(graph.vertexMeta(srcLabel).toReference, null, arguments.columnNameForSourceVertexId), sourceVertexData, preferNewVertexData = false)

      // refresh graph from DB for building self-to-self graph edge
      graph = graphs.expectSeamless(edgeFrameEntity.graphId.get)
      addVerticesPlugin.addVertices(sc, AddVerticesArgs(graph.vertexMeta(destLabel).toReference, null, arguments.columnNameForDestVertexId), destVertexData, preferNewVertexData = false)
    }

    // load src and dest vertex ids
    val srcVertexIds = graphs.loadVertexRDD(sc, graph.id, srcLabel).idColumns.groupByKey()
    srcVertexIds.cache()
    val destVertexIds = if (srcLabel == destLabel) {
      srcVertexIds
    }
    else {
      graphs.loadVertexRDD(sc, graph.id, destLabel).idColumns.groupByKey()
    }

    // check that at least some source and destination vertices actually exist
    if (srcVertexIds.count() == 0) {
      throw new IllegalArgumentException("Source vertex frame does NOT contain any vertices.  Please add source vertices first or enable create_missing_vertices=True.")
    }
    if (destVertexIds.count() == 0) {
      throw new IllegalArgumentException("Destination vertex frame does NOT contain any vertices.  Please add destination vertices first or enable create_missing_vertices=True.")
    }

    // match ids with vertices
    val edgesByTail = edgesWithoutVids.groupByRows(row => row.value(arguments.columnNameForDestVertexId))
    val edgesWithTail = destVertexIds.join(edgesByTail).flatMapValues { value =>
      val idMap = value._1
      val vid = idMap.head
      val edgeRows = value._2
      edgeRows.map(e => edgesWithoutVids.rowWrapper(e).setValue("_dest_vid", vid))
    }.values

    val edgesByHead = new FrameRDD(edgesWithoutVids.frameSchema, edgesWithTail).groupByRows(row => row.value(arguments.columnNameForSourceVertexId))
    val edgesWithVids = srcVertexIds.join(edgesByHead).flatMapValues(value => {
      val idMap = value._1
      val vid = idMap.head
      val edges = value._2
      edges.map(e => edgesWithoutVids.rowWrapper(e).setValue("_src_vid", vid))
    }).values

    srcVertexIds.unpersist(blocking = false)
    destVertexIds.unpersist(blocking = false)
    edgesWithoutVids.unpersist(blocking = false)

    // convert convert edges to add to correct schema
    val correctedSchema = edgesWithoutVids.frameSchema
      //.convertType("_src_vid", DataTypes.int64)
      //.convertType("_dest_vid", DataTypes.int64)
      .dropColumns(List(arguments.columnNameForSourceVertexId, arguments.columnNameForDestVertexId))
    val edgesToAdd = new FrameRDD(edgesWithoutVids.frameSchema, edgesWithVids).convertToNewSchema(correctedSchema)

    // append to existing data
    val existingEdgeData = graphs.loadEdgeRDD(sc, edgeFrameEntity.id)
    val combinedRdd = existingEdgeData.append(edgesToAdd)
    graphs.saveEdgeRdd(edgeFrameEntity.id, combinedRdd)

    // results
    new UnitReturn
  }

}
