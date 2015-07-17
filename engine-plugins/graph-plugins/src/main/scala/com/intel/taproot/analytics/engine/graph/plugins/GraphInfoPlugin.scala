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

package com.intel.taproot.analytics.engine.graph.plugins

import com.intel.taproot.analytics.domain.graph.{ SeamlessGraphMeta, GraphNoArgs }
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, CommandPlugin, Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.graph.SparkGraphStorage

// Implicits needed for JSON conversion
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

/**
 * Debug information for a graph
 */
@PluginDoc(oneLine = "Get debug info about a graph.",
  extended = "")
class GraphInfoPlugin extends CommandPlugin[GraphNoArgs, SeamlessGraphMeta] {

  /**
   * The name of the command, e.g. graph/sampling/vertex_sample
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:/_info"

  /**
   * Debug information for a graph
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments passed by the user
   * @return
   */
  override def execute(arguments: GraphNoArgs)(implicit invocation: Invocation): SeamlessGraphMeta = {
    val graphs = engine.graphs.asInstanceOf[SparkGraphStorage]
    graphs.expectSeamless(arguments.graph.id)
  }

}
