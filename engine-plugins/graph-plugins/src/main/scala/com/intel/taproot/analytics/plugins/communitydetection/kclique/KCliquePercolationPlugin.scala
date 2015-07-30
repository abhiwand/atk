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

package com.intel.taproot.analytics.plugins.communitydetection.kclique

import java.util.Date
import com.intel.taproot.analytics.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.taproot.analytics.domain.frame.FrameEntity
import com.intel.taproot.analytics.domain.{ UserPrincipal, CreateEntityArgs }
import com.intel.taproot.analytics.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import com.intel.taproot.analytics.component.Boot
import com.intel.taproot.analytics.domain.command.CommandDoc
import com.intel.taproot.analytics.domain.graph.GraphReference
import com.intel.taproot.analytics.engine.{ SparkContextFactory, EngineConfig }
import com.intel.taproot.analytics.engine.graph.{ SparkGraph, SparkGraphHBaseBackend, GraphBuilderConfigFactory }
import com.intel.taproot.analytics.engine.plugin.{ SparkCommandPlugin, SparkInvocation }
import org.apache.spark.frame.FrameRdd

import scala.concurrent._

/**
 * Represents the arguments for KClique Percolation algorithm
 *
 * @param graph Reference to the graph for which communities has to be determined.
 */
case class KCliqueArgs(graph: GraphReference,
                       @ArgDoc("""The sizes of the cliques used to form communities.
Larger values of clique size result in fewer, smaller communities that are more connected.
Must be at least 2.""") cliqueSize: Int,
                       @ArgDoc("""Name of the community property of vertex that will be updated/created in the graph.
This property will contain for each vertex the set of communities that contain that
vertex.""") communityPropertyLabel: String) {
  require(cliqueSize > 1, "Invalid clique size; must be at least 2")
}

case class KCliqueResult(frameDictionaryOutput: Map[String, FrameEntity], time: Double)

/**
 * Json conversion for arguments and return value case classes
 */

object KCliquePercolationJsonFormat {
  import com.intel.taproot.analytics.domain.DomainJsonProtocol._
  implicit val kcliqueFormat = jsonFormat3(KCliqueArgs)
  implicit val kcliqueResultFormat = jsonFormat2(KCliqueResult)
}

import KCliquePercolationJsonFormat._
/**
 * KClique Percolation plugin class.
 */

@PluginDoc(oneLine = "Find groups of vertices with similar attributes.",
  extended = """Notes
-----
Spawns a number of Spark jobs that cannot be calculated before execution
(it is bounded by the diameter of the clique graph derived from the input graph).
For this reason, the initial loading, clique enumeration and clique-graph
construction steps are tracked with a single progress bar (this is most of
the time), and then successive iterations of analysis of the clique graph
are tracked with many short-lived progress bars, and then finally the
result is written out.""",
  returns = "Dictionary of vertex label and frame, Execution time."
)
class KCliquePercolationPlugin extends SparkCommandPlugin[KCliqueArgs, KCliqueResult] {

  /**
   * The name of the command, e.g. graphs/ml/kclique_percolation
   */
  override def name: String = "graph:/ml/kclique_percolation"

  /**
   * The number of jobs varies with the number of supersteps required to find the connected components
   * of the derived clique-shadow graph.... we cannot properly anticipate this without doing a full analysis of
   * the graph.
   *
   * @param arguments command arguments: used if a command can produce variable number of jobs
   * @return number of jobs in this command
   */
  override def numberOfJobs(arguments: KCliqueArgs)(implicit invocation: Invocation): Int = {
    8 + 2 * arguments.cliqueSize
  }

  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: KCliqueArgs)(implicit invocation: Invocation): KCliqueResult = {

    val start = System.currentTimeMillis()

    // Get the graph
    val graph: SparkGraph = arguments.graph
    val (gbVertices, gbEdges) = graph.gbRdds
    val (outVertices, outEdges) = KCliquePercolationRunner.run(gbVertices, gbEdges, arguments.cliqueSize, arguments.communityPropertyLabel)

    val mergedVertexRdd = (outVertices ++ gbVertices).mergeDuplicates()

    // Get the execution time and print it
    val time = (System.currentTimeMillis() - start).toDouble / 1000.0

    val frameRddMap = FrameRdd.toFrameRddMap(mergedVertexRdd)

    val frameMap = frameRddMap.keys.map(label => {
      val result = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by connected components operation"))) { newOutputFrame: FrameEntity =>
        val frameRdd = frameRddMap(label)
        newOutputFrame.save(frameRdd)
      }
      (label, result)
    }).toMap
    KCliqueResult(frameMap, time)

  }

}
