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

package com.intel.taproot.atk.graph.plugins

import com.intel.taproot.analytics.UnitReturn
import com.intel.taproot.analytics.domain.graph.construction.AddVerticesArgs
import com.intel.taproot.analytics.domain.schema.GraphSchema
import com.intel.taproot.analytics.engine.frame.SparkFrame
import com.intel.taproot.analytics.engine.graph.SparkVertexFrame
import com.intel.taproot.analytics.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.storage.StorageLevel

// Implicits needed for JSON conversion
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

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
    val sourceFrame: SparkFrame = arguments.sourceFrame
    sourceFrame.schema.validateColumnsExist(arguments.allColumnNames)
    addVertices(arguments, sourceFrame.rdd)
  }

  /**
   * Add vertices
   * @param arguments user supplied arguments
   * @param preferNewVertexData true to prefer new vertex data, false to prefer existing vertex data - during merge
   *                            false is useful for createMissingVertices, otherwise you probably always want true.
   */
  def addVertices(arguments: AddVerticesArgs, sourceRdd: FrameRdd, preferNewVertexData: Boolean = true)(implicit invocation: Invocation): Unit = {

    val vertexFrame: SparkVertexFrame = arguments.vertexFrame
    val vertexDataToAdd = sourceRdd.selectColumns(arguments.allColumnNames)

    // handle id column
    val idColumnName = vertexFrame.schema.determineIdColumnName(arguments.idColumnName)
    val vertexDataWithIdColumn = vertexDataToAdd.renameColumn(arguments.idColumnName, idColumnName)

    // assign unique ids
    val verticesToAdd = vertexDataWithIdColumn.assignUniqueIds(GraphSchema.vidProperty, startId = vertexFrame.graph.nextId)

    verticesToAdd.persist(StorageLevel.MEMORY_AND_DISK)

    vertexFrame.graph.incrementIdCounter(verticesToAdd.count())

    // load existing data, if any, and append the new data
    val existingVertexData = vertexFrame.rdd
    val combinedRdd = existingVertexData.setIdColumnName(idColumnName).append(verticesToAdd, preferNewVertexData)

    combinedRdd.persist(StorageLevel.MEMORY_AND_DISK)

    vertexFrame.save(combinedRdd)

    verticesToAdd.unpersist(blocking = false)
    combinedRdd.unpersist(blocking = false)
  }
}
