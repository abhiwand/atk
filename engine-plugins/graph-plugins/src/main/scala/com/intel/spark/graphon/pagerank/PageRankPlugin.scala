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

package com.intel.spark.graphon.pagerank

import com.intel.intelanalytics.domain.frame.{ FrameMeta, FrameEntity }
import com.intel.intelanalytics.domain.graph.{ GraphReference }
import com.intel.intelanalytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import com.intel.intelanalytics.domain.{ CreateEntityArgs, DomainJsonProtocol }
import org.apache.spark.frame.FrameRdd
import com.intel.intelanalytics.engine.spark.{ SparkContextFactory, EngineConfig }

import spray.json._

/**
 * Parameters for executing page rank.
 * @param graph Reference to the graph object on which to compute pagerank.
 */
case class PageRankArgs(graph: GraphReference,
                        @ArgDoc("""Name of the property to which pagerank value will be stored on vertex and edge.""") output_property: String,
                        @ArgDoc("""List of edge labels to consider for pagerank computation.
If None, all edges are considered.""") input_edge_labels: Option[List[String]] = None,
                        @ArgDoc("""The maximum number of iterations that will be invoked.
Defaults to 20.""") max_iterations: Option[Int] = None,
                        @ArgDoc("""Random reset probability.""") reset_probability: Option[Double] = None,
                        @ArgDoc("""Tolerance allowed at convergence (smaller values tend to yield accurate results).""") convergence_tolerance: Option[Double] = None) {
  require(!output_property.isEmpty, "Output property label must be provided")
}

/**
 * Companion object holds the default values.
 */
object PageRankDefaults {
  val maxIterationsDefault = 20
  val resetProbabilityDefault = 0.15d
  val convergenceToleranceDefault = 0.001d
}

case class PageRankResult(vertexDictionaryOutput: Map[String, FrameEntity], edgeDictionaryOutput: Map[String, FrameEntity])

/** Json conversion for arguments and return value case classes */
object PageRankJsonFormat {
  import DomainJsonProtocol._
  implicit val PRFormat = jsonFormat6(PageRankArgs)
  implicit val PRResultFormat = jsonFormat2(PageRankResult)
}

import PageRankJsonFormat._

@PluginDoc(oneLine = "The pagerank computation on a graph by invoking graphx pagerank.",
  extended = """Pulls graph from underlying store, sends it off to the PageRankRunner, and then writes the output graph
back to the underlying store.

Right now it is using only Titan for graph storage. Other backends including Parquet will be supported later.""")
class PageRankPlugin extends SparkCommandPlugin[PageRankArgs, PageRankResult] {

  override def name: String = "graph/graphx_pagerank"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: PageRankArgs)(implicit invocation: Invocation): PageRankResult = {

    // Titan Settings for input
    val config = configuration

    // Get the graph
    val graph = engine.graphs.expectGraph(arguments.graph)

    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)

    val prRunnerArgs = PageRankRunnerArgs(arguments.output_property,
      arguments.input_edge_labels,
      arguments.max_iterations,
      arguments.reset_probability,
      arguments.convergence_tolerance)

    // Call PageRankRunner to kick off PageRank computation on RDDs
    val (outVertices, outEdges) = PageRankRunner.run(gbVertices, gbEdges, prRunnerArgs)

    val edgeFrameRddMap = FrameRdd.toFrameRddMap(outEdges, outVertices)

    val edgeMap = edgeFrameRddMap.keys.map(edgeLabel => {
      val edgeFrame = tryNew(CreateEntityArgs(description = Some("created by connected components operation"))) { newOutputFrame: FrameMeta =>
        val frameRdd = edgeFrameRddMap(edgeLabel)
        save(new SparkFrameData(newOutputFrame.meta, frameRdd))
      }.meta
      (edgeLabel, edgeFrame)
    }).toMap

    val vertexFrameRddMap = FrameRdd.toFrameRddMap(outVertices)

    val vertexMap = vertexFrameRddMap.keys.map(vertexLabel => {
      val vertexFrame = tryNew(CreateEntityArgs(description = Some("created by connected components operation"))) { newOutputFrame: FrameMeta =>
        val frameRdd = vertexFrameRddMap(vertexLabel)
        save(new SparkFrameData(newOutputFrame.meta, frameRdd))
      }.meta
      (vertexLabel, vertexFrame)
    }).toMap

    new PageRankResult(vertexMap, edgeMap)

  }

}
