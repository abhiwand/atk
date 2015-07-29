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

package com.intel.taproot.analytics.algorithm.graph

import com.intel.taproot.giraph.algorithms.pr.PageRankComputation
import com.intel.taproot.giraph.algorithms.pr.PageRankComputation.{ PageRankMasterCompute, PageRankAggregatorWriter }
import com.intel.taproot.giraph.io.titan.formats.{ TitanVertexOutputFormatLongIDDoubleValue, TitanVertexInputFormatLongDoubleNull }
import com.intel.taproot.analytics.domain.DomainJsonProtocol
import com.intel.taproot.analytics.domain.graph.GraphReference
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, CommandPlugin, Invocation, PluginDoc }
import com.intel.taproot.analytics.algorithm.util.{ GiraphJobManager, GiraphConfigurationUtil }
import org.apache.giraph.conf.GiraphConfiguration
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent.duration._

import scala.concurrent._

case class PageRank(graph: GraphReference,
                    @ArgDoc("""The name(s) of edge label(s).""") inputEdgeLabelList: List[String],
                    @ArgDoc("""Vertex properties to store output vertex values.""") outputVertexPropertyList: List[String],
                    @ArgDoc("""The maximum number of supersteps that the algorithm will execute.
The valid range is all positive int.
The default value is 20.""") maxSupersteps: Option[Int] = None,
                    @ArgDoc("""The amount of change in cost function that will be tolerated at convergence.
If the change is less than this threshold, the algorithm exits earlier,
before it reaches the maximum number of supersteps.
The valid range is all float and zero.
The default value is 0.001.""") convergenceThreshold: Option[Double] = None,
                    @ArgDoc("""The probability that the random walk of a page is reset.""") resetProbability: Option[Double] = None,
                    @ArgDoc("""The convergence progress output interval.
The valid value range is [1, max_supersteps].
The default value is 1, which means output every superstep.""") convergenceProgressOutputInterval: Option[Int] = None) {
}

case class PageRankResult(value: String) //TODO

/** Json conversion for arguments and return value case classes */
object PageRankJsonFormat {
  import DomainJsonProtocol._
  implicit val prFormat = jsonFormat7(PageRank)
  implicit val prResultFormat = jsonFormat1(PageRankResult)
}

import PageRankJsonFormat._
@PluginDoc(oneLine = "Determining which vertices are the most important.",
  extended = """The `PageRank algorithm <http://en.wikipedia.org/wiki/PageRank>`_.

**Basics and Background**

*PageRank* is a method for determining which vertices in a directed graph are
the most central or important.
*PageRank* gives each vertex a score which can be interpreted as the
probability that a person randomly walking along the edges of the graph will
visit that vertex.

The calculation of *PageRank* is based on the supposition that if a vertex has
many vertices pointing to it, then it is "important",
and that a vertex grows in importance as more important vertices point to it.
The calculation is based only on the network structure of the graph and makes
no use of any side data, properties, user-provided scores or similar
non-topological information.

*PageRank* was most famously used as the core of the Google search engine for
many years, but as a general measure of :term:`centrality` in a graph, it has
other uses to other problems, such as :term:`recommendation systems` and
analyzing predator-prey food webs to predict extinctions.

**Background references**

*   Basic description and principles: `Wikipedia\: PageRank`_
*   Applications to food web analysis: `Stanford\: Applications of PageRank`_
*   Applications to recommendation systems: `PLoS\: Computational Biology`_

**Mathematical Details of PageRank Implementation**

Our implementation of *PageRank* satisfies the following equation at each
vertex :math:`v` of the graph:

.. math::

    PR(v) = \frac {\rho}{n} + \rho \left( \sum_{u\in InSet(v)} \
    \frac {PR(u)}{L(u)} \right)

Where:
    |   :math:`v` |EM| a vertex
    |   :math:`L(v)` |EM| outbound degree of the vertex v
    |   :math:`PR(v)` |EM| *PageRank* score of the vertex v
    |   :math:`InSet(v)` |EM| set of vertices pointing to the vertex v
    |   :math:`n` |EM| total number of vertices in the graph
    |   :math:`\rho` - user specified damping factor (also known as reset
        probability)

Termination is guaranteed by two mechanisms.

*   The user can specify a convergence threshold so that the algorithm will
    terminate when, at every vertex, the difference between successive
    approximations to the *PageRank* score falls below the convergence
    threshold.
*   The user can specify a maximum number of iterations after which the
    algorithm will terminate.

.. _Wikipedia\: PageRank: http://en.wikipedia.org/wiki/PageRank
.. _Stanford\: Applications of PageRank: http://web.stanford.edu/class/msande233/handouts/lecture8.pdf
.. _PLoS\: Computational Biology:
    http://www.ploscompbiol.org/article/fetchObject.action?uri=info%3Adoi%2F10.1371%2Fjournal.pcbi.1000494&representation=PDF""",
  returns = """The configuration and convergence report for Pagerank in a multiple-line
  string.""")
class PageRankPlugin
    extends CommandPlugin[PageRank, PageRankResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:titan/page_rank"

  override def execute(arguments: PageRank)(implicit invocation: Invocation): PageRankResult = {
    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")

    val graphFuture = engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "pr.maxSupersteps", arguments.maxSupersteps)
    GiraphConfigurationUtil.set(hConf, "pr.convergenceThreshold", arguments.convergenceThreshold)
    GiraphConfigurationUtil.set(hConf, "pr.resetProbability", arguments.resetProbability)
    GiraphConfigurationUtil.set(hConf, "pr.convergenceProgressOutputInterval", arguments.convergenceProgressOutputInterval)

    GiraphConfigurationUtil.set(hConf, "giraphjob.maxSteps", arguments.maxSupersteps)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, config, graph)

    val argSeparator = ","
    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", Some(arguments.inputEdgeLabelList.mkString(argSeparator)))
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", Some(arguments.outputVertexPropertyList.mkString(argSeparator)))

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanVertexInputFormatLongDoubleNull])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatLongIDDoubleValue[_ <: org.apache.hadoop.io.LongWritable, _ <: org.apache.hadoop.io.DoubleWritable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[PageRankMasterCompute])
    giraphConf.setComputationClass(classOf[PageRankComputation])
    giraphConf.setAggregatorWriterClass(classOf[PageRankAggregatorWriter])

    PageRankResult(GiraphJobManager.run("ia_giraph_pr",
      classOf[PageRankComputation].getCanonicalName,
      config, giraphConf, invocation, "pr-convergence-report_0"))

  }

}
