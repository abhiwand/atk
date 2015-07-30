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
import com.intel.taproot.analytics.domain.graph.DefineEdgeArgs
import com.intel.taproot.analytics.engine.plugin.{ CommandPlugin, Invocation, PluginDoc }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

/**
 * Parameters
 * ----------
 * label : str
 *   label of the edge type
 * source_vertex_label : str
 *   label of the source vertex type
 * destination_vertex_label : str
 *   label of the destination vertex type
 * directed : bool
 *   is the edge directed
 */
/**
 * Define an edge type for a seamless graph
 */
@PluginDoc(oneLine = "Define an edge type.",
  extended = "")
class DefineEdgePlugin extends CommandPlugin[DefineEdgeArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graph/vertex_sample
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:/define_edge_type"

  /**
   * Define edge type
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments specification of the edge type
   * @return data frame which represent the edge of this type
   */
  override def execute(arguments: DefineEdgeArgs)(implicit invocation: Invocation): UnitReturn = {
    engine.graphs.defineEdgeType(arguments.graphRef, arguments.edgeSchema)
  }

}
