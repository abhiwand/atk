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

import com.intel.intelanalytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.engine.spark.frame._
import com.intel.intelanalytics.domain.schema._
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage, PythonRddStorage }
import com.intel.intelanalytics.domain.graph.SeamlessGraphMeta
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.domain.FilterVerticesArgs

import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

@PluginDoc(oneLine = "",
  extended = "")
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
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
