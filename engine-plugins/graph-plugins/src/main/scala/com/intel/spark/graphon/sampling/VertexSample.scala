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

package com.intel.spark.graphon.sampling

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.domain.frame.FrameName
import com.intel.intelanalytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.intelanalytics.engine.spark.graph.GraphBackendName
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.graph.GraphBuilderConfigFactory
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.{ StorageFormats, DomainJsonProtocol }
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphReference }
import spray.json._
import scala.concurrent._
import java.util.UUID
import com.intel.spark.graphon.sampling.VertexSampleSparkOps._
import com.intel.intelanalytics.domain.command.CommandDoc

/**
 * Represents the arguments for vertex sampling
 *
 * @param graph reference to the graph to be sampled
 */
case class VertexSampleArguments(graph: GraphReference,
                                 @ArgDoc("The number of vertices to sample from the graph.") size: Int,
                                 @ArgDoc("The type of vertex sample among: ['uniform', 'degree', 'degreedist'].") sampleType: String,
                                 @ArgDoc("Random seed value.") seed: Option[Long] = None) {
  require(size >= 1, "Invalid sample size")
  require(sampleType.equals("uniform") ||
    sampleType.equals("degree") ||
    sampleType.equals("degreedist"), "Invalid sample type")
}

/**
 * The result object
 *
 * Note: For now, return the subgraph name, since the current state of things requires the name in order to return a
 * new Frame instance in Python.
 *
 * @param name name of the subgraph
 */
case class VertexSampleResult(name: String)

/** Json conversion for arguments and return value case classes */
object VertexSampleJsonFormat {
  import DomainJsonProtocol._
  implicit val vertexSampleFormat = jsonFormat4(VertexSampleArguments)
  implicit val vertexSampleResultFormat = jsonFormat1(VertexSampleResult)
}

import VertexSampleJsonFormat._
@PluginDoc(oneLine = "Make subgraph from vertex sampling.",
  extended = """Create a vertex induced subgraph obtained by vertex sampling.
Three types of vertex sampling are provided: 'uniform', 'degree', and
'degreedist'.
A 'uniform' vertex sample is obtained by sampling vertices uniformly at random.
For 'degree' vertex sampling, each vertex is weighted by its out-degree.
For 'degreedist' vertex sampling, each vertex is weighted by the total
number of vertices that have the same out-degree as it.
That is, the weight applied to each vertex for 'degreedist' vertex sampling
is given by the out-degree histogram bin size.""",
  returns = "A new Graph object representing the vertex induced subgraph.")
class VertexSample extends SparkCommandPlugin[VertexSampleArguments, VertexSampleResult] {

  /**
   * The name of the command
   */
  override def name: String = "graph:titan/sampling/vertex_sample"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: VertexSampleArguments)(implicit invocation: Invocation): VertexSampleResult = {

    // get the input graph object
    val graph = engine.graphs.expectGraph(arguments.graph)

    // convert graph name and get the graph vertex and edge RDDs
    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)

    val vertexSample = arguments.sampleType match {
      case "uniform" => sampleVerticesUniform(gbVertices, arguments.size, arguments.seed)
      case "degree" => sampleVerticesDegree(gbVertices, gbEdges, arguments.size, arguments.seed)
      case "degreedist" => sampleVerticesDegreeDist(gbVertices, gbEdges, arguments.size, arguments.seed)
      case _ => throw new IllegalArgumentException("Invalid sample type")
    }

    // get the vertex induced subgraph edges
    val edgeSample = vertexInducedEdgeSet(vertexSample, gbEdges)

    // strip '-' character so UUID format is consistent with the Python generated UUID format
    val subgraphName = Some(FrameName.generate(prefix = Some("graph_")))

    val subgraph = engine.graphs.createGraph(GraphTemplate(subgraphName, StorageFormats.HBaseTitan))

    // create titan config copy for subgraph write-back
    val subgraphTitanConfig = GraphBuilderConfigFactory.getTitanConfiguration(subgraph.storage)

    writeToTitan(vertexSample, edgeSample, subgraphTitanConfig)

    VertexSampleResult(subgraphName.get)
  }

}
