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
import com.intel.intelanalytics.domain.graph.construction.AddVerticesArgs
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
import com.intel.intelanalytics.domain.schema.{ GraphSchema, VertexSchema }
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage, RowWrapper }
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.storage.StorageLevel

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Parameters
 * ----------
 * source_frame : Frame
 *   frame that will be the source of the vertex data
 * id_column_name : str
 *   column name for a unique id for each vertex
 * column_names : list of str
 *   column names that will be turned into properties for each vertex
 */

/**
 * Add Vertices to a Vertex Frame
 */
@PluginDoc(oneLine = "Add vertices to a graph.",
  extended = "Includes appending to a list of existing vertices.")
class AddVerticesPlugin extends SparkCommandPlugin[AddVerticesArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graph/sampling/vertex_sample
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:vertex/add_vertices"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: AddVerticesArgs)(implicit invocation: Invocation) = 6

  /**
   * Add Vertices to a Seamless Graph
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: AddVerticesArgs)(implicit invocation: Invocation): UnitReturn = {
    val frames = engine.frames
    val graphs = engine.graphs

    // validate arguments
    val sourceFrameEntity = frames.expectFrame(arguments.sourceFrame)
    sourceFrameEntity.schema.validateColumnsExist(arguments.allColumnNames)

    // run the operation
    val sourceRdd = frames.loadFrameData(sc, sourceFrameEntity)
    addVertices(sc, frames, graphs, arguments, sourceRdd)

    new UnitReturn
  }

  /**
   * Add vertices
   * @param sc spark context
   * @param arguments user supplied arguments
   * @param sourceRdd source data
   * @param preferNewVertexData true to prefer new vertex data, false to prefer existing vertex data - during merge
   *                            false is useful for createMissingVertices, otherwise you probably always want true.
   */
  def addVertices(sc: SparkContext, frames: SparkFrameStorage, graphs: SparkGraphStorage, arguments: AddVerticesArgs, sourceRdd: FrameRdd, preferNewVertexData: Boolean = true)(implicit invocation: Invocation): Unit = {
    // validate arguments
    val vertexFrameMeta = frames.expectFrame(arguments.vertexFrame)
    require(vertexFrameMeta.isVertexFrame, "add vertices requires a vertex frame")
    val graph = graphs.expectSeamless(vertexFrameMeta.graphId.get)
    val graphRef = GraphReference(graph.id)

    val vertexDataToAdd = sourceRdd.selectColumns(arguments.allColumnNames)

    // handle id column
    val idColumnName = vertexFrameMeta.schema.asInstanceOf[VertexSchema].determineIdColumnName(arguments.idColumnName)
    val vertexDataWithIdColumn = vertexDataToAdd.renameColumn(arguments.idColumnName, idColumnName)

    // assign unique ids
    val verticesToAdd = vertexDataWithIdColumn.assignUniqueIds(GraphSchema.vidProperty, startId = graph.nextId())

    verticesToAdd.persist(StorageLevel.MEMORY_AND_DISK)

    graphs.updateIdCounter(graphRef, verticesToAdd.count())

    // load existing data, if any, and append the new data
    val existingVertexData = graphs.loadVertexRDD(sc, vertexFrameMeta.toReference)
    val combinedRdd = existingVertexData.setIdColumnName(idColumnName).append(verticesToAdd, preferNewVertexData)

    combinedRdd.persist(StorageLevel.MEMORY_AND_DISK)

    graphs.saveVertexRdd(vertexFrameMeta.toReference, combinedRdd)

    verticesToAdd.unpersist(blocking = false)
    combinedRdd.unpersist(blocking = false)
  }
}
