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

package com.intel.spark.graphon.graphclustering

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.graph.{ GraphReference }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.graph.GraphBuilderConfigFactory
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

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

    if (!SparkEngineConfig.isSparkOnYarn)
      sc.addJar(SparkContextFactory.jarPath("graph-plugins"))
    val graph = engine.graphs.expectGraph(arguments.graph)
    val (vertices, edges) = engine.graphs.loadGbElements(sc, graph)
    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph)

    new GraphClusteringWorker(titanConfig).execute(vertices, edges, arguments.edgeDistance)
    new UnitReturn
  }
}
