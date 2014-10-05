////////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
////////////////////////////////////////////////////////////////////////////////

package com.intel.spark.graphon.sampling

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.graph.GraphName
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.DomainJsonProtocol
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
 * @param size the requested sample size
 * @param sampleType type of vertex sampling to use
 * @param seed optional random seed value
 */
case class VertexSampleArguments(graph: GraphReference, size: Int, sampleType: String, seed: Option[Long] = None) {
  require(size >= 1, "Invalid sample size")
  require(sampleType.equals("uniform") ||
    sampleType.equals("degree") ||
    sampleType.equals("degreedist"), "Invalid sample type")
}

/**
 * The result object
 *
 * Note: For now, return the subgraph name, since the current state of things requires the name in order to return a
 * new BigFrame instance in Python.
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

class VertexSample extends SparkCommandPlugin[VertexSampleArguments, VertexSampleResult] {

  /**
   * The name of the command
   */
  override def name: String = "graphs/sampling/vertex_sample"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc = Some(CommandDoc(oneLineSummary = "Create a vertex induced subgraph obtained by vertex sampling.",
    extendedSummary = Some("""
    Three types of vertex sampling are provided: 'uniform', 'degree', and 'degreedist'.  A 'uniform' vertex sample
    is obtained by sampling vertices uniformly at random.  For 'degree' vertex sampling, each vertex is weighted by
    its out-degree.  For 'degreedist' vertex sampling, each vertex is weighted by the total number of vertices that
    have the same out-degree as it.  That is, the weight applied to each vertex for 'degreedist' vertex sampling is
    given by the out-degree histogram bin size.

    Parameters
    ----------
    size : int
        the number of vertices to sample from the graph
    sample_type : str
        the type of vertex sample among: ['uniform', 'degree', 'degreedist']
    seed : (optional) int
        random seed value

    Returns
    -------
    BigGraph
        a new BigGraph object representing the vertex induced subgraph

    Examples
    --------
    Assume a set of rules created on a BigFrame that specifies 'user' and 'product' vertices as well as an edge rule.
    The BigGraph created from this data can be vertex sampled to obtain a vertex induced subgraph::

        graph = BigGraph([user_vertex_rule, product_vertex_rule, edge_rule])
        subgraph = graph.sampling.vertex_sample(1000, 'uniform')
""")))

  override def execute(invocation: SparkInvocation, arguments: VertexSampleArguments)(implicit user: UserPrincipal, executionContext: ExecutionContext): VertexSampleResult = {
    // Titan Settings
    val config = configuration

    // get the input graph object
    import scala.concurrent.duration._
    val graph = Await.result(invocation.engine.getGraph(arguments.graph.id), config.getInt("default-timeout") seconds)

    // create titanConfig
    val titanConfig = SparkEngineConfig.createTitanConfiguration(config, "titan.load")
    val iatGraphName = GraphName.convertGraphUserNameToBackendName(graph.name)
    val titanTableNameKey = TitanGraphConnector.getTitanTableNameKey(titanConfig)
    titanConfig.setProperty(titanTableNameKey, iatGraphName)

    // get SparkContext and add the graphon jar
    val sc = invocation.sparkContext
    sc.addJar(Boot.getJar("graphon").getPath)

    // convert graph name and get the graph vertex and edge RDDs
    val (vertexRDD, edgeRDD) = getGraphRdds(sc, titanConfig)

    val vertexSample = arguments.sampleType match {
      case "uniform" => sampleVerticesUniform(vertexRDD, arguments.size, arguments.seed)
      case "degree" => sampleVerticesDegree(vertexRDD, edgeRDD, arguments.size, arguments.seed)
      case "degreedist" => sampleVerticesDegreeDist(vertexRDD, edgeRDD, arguments.size, arguments.seed)
      case _ => throw new IllegalArgumentException("Invalid sample type")
    }

    // get the vertex induced subgraph edges
    val edgeSample = vertexInducedEdgeSet(vertexSample, edgeRDD)

    // strip '-' character so UUID format is consistent with the Python generated UUID format
    val subgraphName = "graph_" + UUID.randomUUID.toString.filter(c => c != '-')
    val iatSubgraphName = GraphName.convertGraphUserNameToBackendName(subgraphName)

    val subgraph = Await.result(invocation.engine.createGraph(GraphTemplate(subgraphName)), config.getInt("default-timeout") seconds)

    // create titan config copy for subgraph write-back
    val subgraphTitanConfig = new SerializableBaseConfiguration()
    subgraphTitanConfig.copy(titanConfig)

    val subgraphTableNameKey = TitanGraphConnector.getTitanTableNameKey(titanConfig)
    subgraphTitanConfig.setProperty(subgraphTableNameKey, iatSubgraphName)

    writeToTitan(vertexSample, edgeSample, subgraphTitanConfig)

    VertexSampleResult(subgraphName)
  }

}
