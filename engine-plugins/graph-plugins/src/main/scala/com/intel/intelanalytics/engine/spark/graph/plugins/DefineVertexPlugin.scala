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

import com.intel.intelanalytics.domain.graph.{ DefineVertexArgs, SeamlessGraphMeta }
import com.intel.intelanalytics.domain.schema.VertexSchema
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.domain.schema.{ GraphSchema, VertexSchema }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

// Implicits needed for JSON conversion

import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Parameters
 * ----------
 * label : str
 *   label of the vertex type
 */

/**
 * Define a vertex type for a seamless graph
 */
@PluginDoc(oneLine = "Define a vertex type by label.",
  extended = "")
class DefineVertexPlugin extends CommandPlugin[DefineVertexArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graph/sampling/vertex_sample
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:/define_vertex_type"

  /**
   * Define vertex type
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments specification of the vertex type
   * @return data frame which represent the vertex of this type
   */
  override def execute(arguments: DefineVertexArgs)(implicit invocation: Invocation): UnitReturn = {
    engine.graphs.defineVertexType(arguments.graphRef, VertexSchema(GraphSchema.vertexSystemColumns, arguments.label, None))
    UnitReturn()
  }

}
