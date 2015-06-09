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

package com.intel.intelanalytics.engine.spark.graph.plugins

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.domain.graph.construction.{ AddEdgesArgs, AddVerticesArgs }
import com.intel.intelanalytics.domain.schema.{ GraphSchema, DataTypes, EdgeSchema }
import com.intel.intelanalytics.engine.plugin.{ CommandInvocation, Invocation }
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage }
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import com.intel.intelanalytics.engine.spark.frame.{ RowWrapper }
import org.apache.spark.frame.FrameRdd
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.SparkContext._

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
/**
 * Parameters
 * ----------
 * source_frame : Frame
 *   frame that will be the source of the edge data
 * column_name_for_src_vertex_id : str
 *   column name for a unique id for each source vertex (this is not the system
 *   defined _vid)
 * column_name_for_dest_vertex_id : str
 *   column name for a unique id for each destination vertex (this is not the
 *   system defined _vid)
 * column_names : list of str
 *   column names that will be turned into properties for each edge
 * create_missing_vertices : Boolean (optional)
 *   True to create missing vertices for edge (slightly slower), False to drop
 *   edges pointing to missing vertices.
 *   Defaults to False.
 */

/**
 * Add Vertices to a Vertex Frame
 */
@PluginDoc(oneLine = "Add edges to a graph.",
  extended = "Includes appending to a list of existing edges.",
  returns = "")
class AddEdgesPlugin extends SparkCommandPlugin[AddEdgesArgs, UnitReturn] {

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
    var graphRef = GraphReference(graph.id)
    val sourceFrameMeta = frames.expectFrame(arguments.sourceFrame)
    sourceFrameMeta.schema.validateColumnsExist(arguments.allColumnNames)

    // run the operation

    // load source data
    val sourceRdd = frames.loadFrameData(sc, sourceFrameMeta)

    // assign unique ids to source data
    val edgeDataToAdd = sourceRdd.selectColumns(arguments.allColumnNames).assignUniqueIds(GraphSchema.edgeProperty, startId = graph.nextId())
    edgeDataToAdd.cache()
    graphs.updateIdCounter(graphRef, edgeDataToAdd.count())

    // convert to appropriate schema, adding edge system columns
    val edgesWithoutVids = edgeDataToAdd.convertToNewSchema(edgeDataToAdd.frameSchema.addColumn(GraphSchema.srcVidProperty, DataTypes.int64).addColumn(GraphSchema.destVidProperty, DataTypes.int64))
    edgesWithoutVids.cache()
    edgeDataToAdd.unpersist(blocking = false)

    val srcLabel = edgeFrameEntity.schema.asInstanceOf[EdgeSchema].srcVertexLabel
    val destLabel = edgeFrameEntity.schema.asInstanceOf[EdgeSchema].destVertexLabel

    // create vertices from edge data and append to vertex frames
    if (arguments.isCreateMissingVertices) {
      val sourceVertexData = edgesWithoutVids.selectColumns(List(arguments.columnNameForSourceVertexId))
      val destVertexData = edgesWithoutVids.selectColumns(List(arguments.columnNameForDestVertexId))
      val addVerticesPlugin = new AddVerticesPlugin
      addVerticesPlugin.addVertices(sc, frames, graphs, AddVerticesArgs(graph.vertexMeta(srcLabel).toReference, null, arguments.columnNameForSourceVertexId), sourceVertexData, preferNewVertexData = false)

      // refresh graph from DB for building self-to-self graph edge
      graph = graphs.expectSeamless(edgeFrameEntity.graphId.get)
      addVerticesPlugin.addVertices(sc, frames, graphs, AddVerticesArgs(graph.vertexMeta(destLabel).toReference, null, arguments.columnNameForDestVertexId), destVertexData, preferNewVertexData = false)
    }

    // load src and dest vertex ids
    graphRef = GraphReference(graph.id)
    val srcVertexIds = graphs.loadVertexRDD(sc, graphRef, srcLabel).idColumns.groupByKey()
    srcVertexIds.cache()
    val destVertexIds = if (srcLabel == destLabel) {
      srcVertexIds
    }
    else {
      graphs.loadVertexRDD(sc, graphRef, destLabel).idColumns.groupByKey()
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
      edgeRows.map(e => edgesWithoutVids.rowWrapper(e).setValue(GraphSchema.destVidProperty, vid))
    }.values

    val edgesByHead = new FrameRdd(edgesWithoutVids.frameSchema, edgesWithTail).groupByRows(row => row.value(arguments.columnNameForSourceVertexId))
    val edgesWithVids = srcVertexIds.join(edgesByHead).flatMapValues(value => {
      val idMap = value._1
      val vid = idMap.head
      val edges = value._2
      edges.map(e => edgesWithoutVids.rowWrapper(e).setValue(GraphSchema.srcVidProperty, vid))
    }).values

    srcVertexIds.unpersist(blocking = false)
    destVertexIds.unpersist(blocking = false)
    edgesWithoutVids.unpersist(blocking = false)

    // convert convert edges to add to correct schema
    val correctedSchema = edgesWithoutVids.frameSchema
      //.convertType("_src_vid", DataTypes.int64)
      //.convertType("_dest_vid", DataTypes.int64)
      .dropColumns(List(arguments.columnNameForSourceVertexId, arguments.columnNameForDestVertexId))
    val edgesToAdd = new FrameRdd(edgesWithoutVids.frameSchema, edgesWithVids).convertToNewSchema(correctedSchema)

    // append to existing data
    val existingEdgeData = graphs.loadEdgeRDD(sc, edgeFrameEntity.toReference)
    val combinedRdd = existingEdgeData.append(edgesToAdd)
    graphs.saveEdgeRdd(edgeFrameEntity.toReference, combinedRdd)

    // results
    new UnitReturn
  }

}
