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

package com.intel.taproot.atk.graph.plugins.trianglecount

import com.intel.taproot.analytics.domain.frame.FrameEntity
import com.intel.taproot.analytics.domain.{ CreateEntityArgs, DomainJsonProtocol }
import com.intel.taproot.analytics.domain.graph.GraphReference
import com.intel.taproot.analytics.engine.graph.SparkGraph
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import com.intel.taproot.analytics.engine.{ SparkContextFactory, EngineConfig }

import com.intel.taproot.analytics.domain.DomainJsonProtocol._
import spray.json._

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

    val graph: SparkGraph = arguments.graph
    val (gbVertices, gbEdges) = graph.gbRdds

    val tcRunnerArgs = TriangleCountRunnerArgs(arguments.output_property, arguments.input_edge_labels)

    // Call TriangleCountRunner to kick off Triangle Count computation on RDDs
    val (outVertices, outEdges) = TriangleCountRunner.run(gbVertices, gbEdges, tcRunnerArgs)

    val frameRddMap = FrameRdd.toFrameRddMap(outVertices)

    new TriangleCountResult(frameRddMap.keys.map(label => {
      val result = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by connected components operation"))) { newOutputFrame: FrameEntity =>
        val frameRdd = frameRddMap(label)
        newOutputFrame.save(frameRdd)
      }
      (label, result)
    }).toMap)

  }

}
