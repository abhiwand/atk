//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.algorithm.graph

import com.intel.giraph.algorithms.pr.PageRankComputation
import com.intel.giraph.algorithms.pr.PageRankComputation.{ PageRankMasterCompute, PageRankAggregatorWriter }
import com.intel.giraph.io.titan.formats.{ TitanVertexOutputFormatLongIDDoubleValue, TitanVertexInputFormatLongDoubleNull }
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.algorithm.util.{ GiraphJobManager, GiraphConfigurationUtil }
import org.apache.giraph.conf.GiraphConfiguration
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent.duration._
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

import scala.concurrent._
import com.intel.intelanalytics.domain.command.CommandDoc

case class PageRank(graph: GraphReference,
                    @ArgDoc("""The name(s) of edge label(s).""")
                    inputEdgeLabelList: List[String],
                    @ArgDoc("""Vertex properties to store output vertex values.""")
                    outputVertexPropertyList: List[String],
                    @ArgDoc("""The maximum number of supersteps that the algorithm will execute.
The valid range is all positive int.
The default value is 20.""")
                    maxSupersteps: Option[Int] = None,
                    @ArgDoc("""The amount of change in cost function that will be tolerated at convergence.
If the change is less than this threshold, the algorithm exits earlier,
before it reaches the maximum number of supersteps.
The valid range is all float and zero.
The default value is 0.001.""")
                    convergenceThreshold: Option[Double] = None,
                    @ArgDoc("""The probability that the random walk of a page is reset.""")
                    resetProbability: Option[Double] = None,
                    @ArgDoc("""The convergence progress output interval.
The valid value range is [1, max_supersteps].
The default value is 1, which means output every superstep.""")
                    convergenceProgressOutputInterval: Option[Int] = None)

case class PageRankResult(value: String) //TODO

/** Json conversion for arguments and return value case classes */
object PageRankJsonFormat {
  import DomainJsonProtocol._
  implicit val prFormat = jsonFormat7(PageRank)
  implicit val prResultFormat = jsonFormat1(PageRankResult)
}

import PageRankJsonFormat._
@PluginDoc(oneLine = "Determining which vertices are the most important.",
  extended = """The `PageRank algorithm <http://en.wikipedia.org/wiki/PageRank>`_.""",
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
