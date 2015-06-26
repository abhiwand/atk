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

package com.intel.spark.graphon.clusteringcoefficient

import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.domain.{ CreateEntityArgs, StorageFormats, DomainJsonProtocol }
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphEntity, GraphReference }
import com.intel.intelanalytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.intelanalytics.engine.spark.{SparkContextFactory, EngineConfig}
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin

case class ClusteringCoefficientArgs(graph: GraphReference,
                                     @ArgDoc("") outputPropertyName: Option[String],
                                     @ArgDoc("") inputEdgeLabels: Option[List[String]] = None) {

  require(graph != null, "graph is required")
  require(outputPropertyName != null, "output property name should not be null")
  require(inputEdgeLabels != null, "list of edge labels should not be null")

  def inputEdgeSet: Option[Set[String]] =
    if (inputEdgeLabels.isEmpty) {
      None
    }
    else {
      Some(inputEdgeLabels.get.toSet)
    }
}

/**
 * Result of clustering coefficient calculation.
 * @param globalClusteringCoefficient The global clustering coefficient of the graph.
 * @param frame If local clustering coefficients are requested, a reference to the frame with local clustering
 *              coefficients stored at properties at each vertex.
 */
case class ClusteringCoefficientResult(globalClusteringCoefficient: Double, frame: Option[FrameEntity] = None)

/** Json conversion for arguments and return value case classes */
object ClusteringCoefficientJsonFormat {
  import com.intel.intelanalytics.domain.DomainJsonProtocol._
  implicit val CCFormat = jsonFormat3(ClusteringCoefficientArgs)
  implicit val CCResultFormat = jsonFormat2(ClusteringCoefficientResult)
}
import ClusteringCoefficientJsonFormat._

@PluginDoc(oneLine = "Coefficient of graph with respect to labels.",
  extended = """Calculates the clustering coefficient of the graph with repect to an (optional) set of labels.

Pulls graph from underlying store, calculates degrees and writes them into the property specified,
and then writes the output graph to the underlying store.

Right now it uses only Titan for graph storage. Other backends will be supported later.""",
  returns = """Dictionary of the global clustering coefficient of the graph or,
if local clustering coefficients are requested, a reference to the frame with local
clustering coefficients stored at properties at each vertex.""")
class ClusteringCoefficientPlugin extends SparkCommandPlugin[ClusteringCoefficientArgs, ClusteringCoefficientResult] {

  override def name: String = "graph/clustering_coefficient"

  override def numberOfJobs(arguments: ClusteringCoefficientArgs)(implicit invocation: Invocation): Int = 6

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: ClusteringCoefficientArgs)(implicit invocation: Invocation): ClusteringCoefficientResult = {

    val frames = engine.frames
    val graphs = engine.graphs

    // Get the graph
    val graph = graphs.expectGraph(arguments.graph)
    val (gbVertices, gbEdges) = graphs.loadGbElements(sc, graph)
    val ccOutput = ClusteringCoefficientRunner.run(gbVertices, gbEdges, arguments.outputPropertyName, arguments.inputEdgeSet)

    if (ccOutput.vertexOutput.isDefined) {
      val newFrame = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("clustering coefficient results"))) {
        newFrame => frames.saveFrameData(newFrame.toReference, ccOutput.vertexOutput.get)
      }
      ClusteringCoefficientResult(ccOutput.globalClusteringCoefficient, Some(newFrame))
    }
    else {
      ClusteringCoefficientResult(ccOutput.globalClusteringCoefficient)
    }

  }

}
