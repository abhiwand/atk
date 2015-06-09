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

package com.intel.spark.graphon.trianglecount

import com.intel.intelanalytics.domain.frame.{ FrameMeta, FrameEntity }
import com.intel.intelanalytics.domain.{ CreateEntityArgs, DomainJsonProtocol }
import com.intel.intelanalytics.domain.graph.{ GraphReference }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import org.apache.spark.frame.FrameRdd
import com.intel.intelanalytics.engine.spark.SparkEngineConfig

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import spray.json._

import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

/**
 * Parameters for executing triangle count.
 * @param graph Reference to the graph object on which to compute triangle count.
 * @param output_property
 * @param input_edge_labels
 */
case class TriangleCountArgs(graph: GraphReference,
                             @ArgDoc("""Name of the property to which triangle count value will be stored on vertex.""") output_property: String,
                             @ArgDoc("""List of edge labels to consider for computation.
If None, all edges are considered.""") input_edge_labels: Option[List[String]] = None) {
  require(!output_property.isEmpty, "Output property label must be provided")
}

/**
 * The result object
 * @param frameDictionaryOutput Name of the output graph
 */
case class TriangleCountResult(frameDictionaryOutput: Map[String, FrameEntity])

/** Json conversion for arguments and return value case classes */
object TriangleCountJsonFormat {
  import DomainJsonProtocol._
  implicit val TCFormat = jsonFormat3(TriangleCountArgs)
  implicit val TCResultFormat = jsonFormat1(TriangleCountResult)
}

import TriangleCountJsonFormat._

@PluginDoc(oneLine = "TriangleCount plugin implements the triangle count computation on a graph by invoking graphx TriangleCount.",
  extended = """Pulls graph from underlying store, sends it off to the TriangleCountRunner.""",
  returns = "Dictionary of vertex label and frame.")
class TriangleCountPlugin extends SparkCommandPlugin[TriangleCountArgs, TriangleCountResult] {

  override def name: String = "graph/graphx_triangle_count"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def numberOfJobs(arguments: TriangleCountArgs)(implicit invocation: Invocation) = 2

  override def execute(arguments: TriangleCountArgs)(implicit invocation: Invocation): TriangleCountResult = {

    if (!SparkEngineConfig.isSparkOnYarn)
      sc.addJar(SparkContextFactory.jarPath("graph-plugins"))

    // Get the graph
    val graph = engine.graphs.expectGraph(arguments.graph)

    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)

    val tcRunnerArgs = TriangleCountRunnerArgs(arguments.output_property, arguments.input_edge_labels)

    // Call TriangleCountRunner to kick off Triangle Count computation on RDDs
    val (outVertices, outEdges) = TriangleCountRunner.run(gbVertices, gbEdges, tcRunnerArgs)

    val frameRddMap = FrameRdd.toFrameRddMap(outVertices)

    new TriangleCountResult(frameRddMap.keys.map(label => {
      val result = tryNew(CreateEntityArgs(description = Some("created by connected components operation"))) { newOutputFrame: FrameMeta =>
        val frameRdd = frameRddMap(label)
        save(new SparkFrameData(newOutputFrame.meta, frameRdd))
      }.meta
      (label, result)
    }).toMap)

  }

}
