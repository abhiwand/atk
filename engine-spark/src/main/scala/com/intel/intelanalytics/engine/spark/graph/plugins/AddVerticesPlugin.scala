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

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.graph.{ SeamlessGraphMeta, Graph }
import com.intel.intelanalytics.domain.graph.construction.AddVertices
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage, FrameRDD, RowWrapper }
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.SparkContext
import org.apache.spark.ia.graph.VertexFrameRDD
import org.apache.spark.sql.SQLContext

import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.domain.frame.FrameReference

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Add Vertices to a Vertex Frame
 */
class AddVerticesPlugin(frames: SparkFrameStorage, graphs: SparkGraphStorage) extends SparkCommandPlugin[AddVertices, UnitReturn] {

  /**
   * The name of the command, e.g. graph/sampling/vertex_sample
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:vertex/add_vertices"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Add vertices to a graph",
    extendedSummary = Some("""
    Add vertices to a graph.  Includes appending to a list of existing vertices.

    Parameters
    ----------
    source_frame: Frame
        frame that will be the source of the vertex data
    id_column_name: str
        column name for a unique id for each vertex
    column_names: list of strings
        column names that will be turned into properties for each vertex

    Examples
    --------
                             |graph = ia.Graph()
                             |graph.define_vertex_type('users')
                             |graph.add_vertices(frame, 'user_id', ['user_name', 'age'])
                           """)))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: AddVertices) = 6

  /**
   * Add Vertices to a Seamless Graph
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: AddVertices)(implicit user: UserPrincipal, executionContext: ExecutionContext): UnitReturn = {
    val ctx = invocation.sparkContext

    // validate arguments
    val sourceFrameMeta = frames.expectFrame(arguments.sourceFrame)
    sourceFrameMeta.schema.validateColumnsExist(arguments.allColumnNames)

    // run the operation
    val sourceRdd = frames.loadFrameRDD(ctx, sourceFrameMeta)
    AddVerticesPlugin.addVertices(ctx, sourceRdd, arguments.vertexFrame, arguments.allColumnNames, arguments.idColumnName, frames, graphs)

    new UnitReturn
  }
}

object AddVerticesPlugin {
  /**
   * Add vertices
   * @param ctx spark context
   * @param sourceRdd source data
   * @param preferNewVertexData true to prefer new vertex data, false to prefer existing vertex data - during merge
   *                            false is useful for createMissingVertices, otherwise you probably always want true.
   */
  def addVertices(ctx: SparkContext, sourceRdd: FrameRDD, vertexFrame: FrameReference, allColumnNames: List[String], idColumnName: String, frames: SparkFrameStorage, graphs: SparkGraphStorage, preferNewVertexData: Boolean = true): Unit = {
    // validate arguments
    val vertexFrameMeta = frames.expectFrame(vertexFrame)
    require(vertexFrameMeta.isVertexFrame, "add vertices requires a vertex frame")
    val graph = graphs.expectSeamless(vertexFrameMeta.graphId.get)

    val vertexDataToAdd = sourceRdd.selectColumns(allColumnNames)

    // handle id column
    val idColumn = vertexFrameMeta.schema.vertexSchema.get.determineIdColumnName(idColumnName)
    val vertexDataWithIdColumn = vertexDataToAdd.renameColumn(idColumn, idColumn)

    // assign unique ids
    val verticesToAdd = vertexDataWithIdColumn.assignUniqueIds("_vid", startId = graph.nextId())
    verticesToAdd.cache()
    graphs.updateIdCounter(graph.id, verticesToAdd.count())

    // load existing data, if any, and append the new data
    val existingVertexData = graphs.loadVertexRDD(ctx, vertexFrameMeta.id)
    val combinedRdd = existingVertexData.setIdColumnName(idColumn).append(verticesToAdd)
    graphs.saveVertexRDD(vertexFrameMeta.id, combinedRdd, Some(combinedRdd.count()))

    verticesToAdd.unpersist(blocking = false)
    combinedRdd.unpersist(blocking = false)
  }
}
