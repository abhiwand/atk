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

package com.intel.taproot.spark.graphon.graphclustering

import com.intel.taproot.analytics.UnitReturn
import com.intel.taproot.analytics.domain.graph.GraphReference
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.{ SparkContextFactory, EngineConfig }
import com.intel.taproot.analytics.engine.graph.{ SparkGraph, GraphBuilderConfigFactory }
import com.intel.taproot.analytics.engine.plugin.SparkCommandPlugin
import com.intel.taproot.analytics.domain.DomainJsonProtocol

// Implicits needed for JSON conversion
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

case class GraphClusteringArgs(graph: GraphReference,
                               @ArgDoc("""Column name for the edge distance.""") edgeDistance: String)

/** Json conversion for arguments and return value case classes */
object GraphClusteringFormat {
  import DomainJsonProtocol._
  implicit val hFormat = jsonFormat2(GraphClusteringArgs)
}

import GraphClusteringFormat._

/**
 * GraphClusteringPlugin implements the graph clustering algorithm on a graph.
 */
@PluginDoc(oneLine = "Build graph clustering over an initial titan graph.",
  extended = "",
  returns = "A set of titan vertices and edges representing the internal clustering of the graph.")
class GraphClusteringPlugin extends SparkCommandPlugin[GraphClusteringArgs, UnitReturn] {

  override def name: String = "graph:titan/graph_clustering"
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: GraphClusteringArgs)(implicit invocation: Invocation): UnitReturn = {
    val graph: SparkGraph = arguments.graph
    val (vertices, edges) = graph.gbRdds
    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph)

    new GraphClusteringWorker(titanConfig).execute(vertices, edges, arguments.edgeDistance)
    new UnitReturn
  }
}
