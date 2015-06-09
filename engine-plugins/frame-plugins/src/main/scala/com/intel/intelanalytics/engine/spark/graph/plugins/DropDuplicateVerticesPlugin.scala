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

import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.engine.spark.frame.plugins.DropDuplicatesPlugin
import com.intel.intelanalytics.domain.FilterVerticesArgs
import com.intel.intelanalytics.domain.frame.{ DropDuplicatesArgs, FrameEntity }
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.ExecutionContext
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage, MiscFrameFunctions, LegacyFrameRdd }
import com.intel.intelanalytics.domain.graph.SeamlessGraphMeta
import org.apache.spark.SparkContext
import com.intel.intelanalytics.domain.schema.{ GraphSchema, VertexSchema, DataTypes }
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage

import org.apache.spark.frame.FrameRdd

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

class DropDuplicateVerticesPlugin extends SparkCommandPlugin[DropDuplicatesArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   *
   * The colon ":" is used to to indicate command destination base classes, default classes or classes of a
   * specific storage type:
   *
   * - graph:titan means command is loaded into class TitanGraph
   * - graph: means command is loaded into class Graph, our default type which will be the Parquet-backed graph
   * - graph would mean command is loaded into class BaseGraph, which applies to all graph classes
   * - frame: and means command is loaded in class Frame.  Example: "frame:/assign_sample"
   * - model:logistic_regression  means command is loaded into class LogisticRegressionModel
   */
  override def name: String = "frame:vertex/drop_duplicates"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: DropDuplicatesArgs)(implicit invocation: Invocation) = 4

  /**
   * Plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DropDuplicatesArgs)(implicit invocation: Invocation): FrameEntity = {
    val frames = engine.frames.asInstanceOf[SparkFrameStorage]
    val graphStorage = engine.graphs
    val vertexFrame = frames.expectFrame(arguments.frame)

    require(vertexFrame.isVertexFrame, "vertex frame is required")

    val seamlessGraph: SeamlessGraphMeta = graphStorage.expectSeamless(vertexFrame.graphId.get)
    val schema = vertexFrame.schema
    val rdd = frames.loadLegacyFrameRdd(sc, arguments.frame)
    val columnNames = arguments.unique_columns match {
      case Some(columns) => vertexFrame.schema.validateColumnsExist(columns.value).toList
      case None =>
        // _vid is always unique so don't include it
        vertexFrame.schema.columnNames.dropWhile(s => s == GraphSchema.vidProperty)
    }
    schema.validateColumnsExist(columnNames)
    val duplicatesRemoved: RDD[Array[Any]] = MiscFrameFunctions.removeDuplicatesByColumnNames(rdd, schema, columnNames)

    val label = schema.asInstanceOf[VertexSchema].label
    FilterVerticesFunctions.removeDanglingEdges(label, frames, seamlessGraph, sc, FrameRdd.toFrameRdd(schema, duplicatesRemoved))

    // save results
    frames.saveLegacyFrame(vertexFrame.toReference, new LegacyFrameRdd(schema, duplicatesRemoved))

  }
}
