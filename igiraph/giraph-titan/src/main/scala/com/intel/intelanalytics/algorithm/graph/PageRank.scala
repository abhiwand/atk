//////////////////////////////////////////////////////////////////////////////
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
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.algorithm.graph

import com.intel.giraph.algorithms.pr.PageRankComputation
import com.intel.giraph.io.titan.TitanVertexOutputFormatLongIDDoubleValue
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatLongDoubleNull
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.algorithm.util.{ GiraphJobManager, GiraphConfigurationUtil }
import org.apache.giraph.conf.GiraphConfiguration
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent.duration._

import scala.concurrent._
import com.intel.intelanalytics.domain.command.CommandDoc

case class Pr(graph: GraphReference,
              input_edge_label_list: Option[String],
              output_vertex_property_list: Option[String],
              max_supersteps: Option[Int] = None,
              convergence_threshold: Option[Double] = None,
              reset_probability: Option[Double] = None,
              convergence_progress_output_interval: Option[Int] = None)

case class PrResult(value: String) //TODO

/** Json conversion for arguments and return value case classes */
object PrJsonFormat {
  import DomainJsonProtocol._
  implicit val prFormat = jsonFormat7(Pr)
  implicit val prResultFormat = jsonFormat1(PrResult)
}

import PrJsonFormat._

class PageRank
    extends CommandPlugin[Pr, PrResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the Python layer via code generation.
   */
  override def name: String = "graphs/ml/page_rank"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc = Some(CommandDoc(oneLineSummary = "The `PageRank algorithm <http://en.wikipedia.org/wiki/PageRank>`_.",
    extendedSummary = Some("""
    Parameters
    ----------
    input_edge_label : String
        The name of edge label.

    output_vertex_property_list : Comma Separated String
        The list of vertex properties to store output vertex values.

    max_supersteps : Integer (optional)
        The maximum number of super steps that the algorithm will execute.
        The valid value range is all positive integer.
        The default value is 20.

    convergence_threshold : Float (optional)
        The amount of change in cost function that will be tolerated at convergence.
        If the change is less than this threshold, the algorithm exists earlier
        before it reaches the maximum number of super steps.
        The valid value range is all Float and zero.
        The default value is 0.001.

    reset_probability : Float (optional)
        The probability that the random walk of a page is reset.

    convergence_output_interval : Integer (optional)
        The convergence progress output interval
        The valid value range is [1, max_supersteps]
        The default value is 1, which means output every super step.

    Returns
    -------
    Multiple line string
        The configuration and convergence report for Page Rank.

    Examples
    --------
    g.ml.page_rank(input_edge_label_list = "edge", output_vertex_property_list = "pr_result")

    The expected output is like this
    {u'value': u'======Graph Statistics======\nNumber of vertices: 20140\nNumber of edges: 604016\n\n======PageRank Configuration======\nmaxSupersteps: 20\nconvergenceThreshold: 0.001000\nresetProbability: 0.150000\nconvergenceProgressOutputInterval: 1\n\n======Learning Progress======\nsuperstep = 1\tsumDelta = 34080.702123\nsuperstep = 2\tsumDelta = 28520.485452\nsuperstep = 3\tsumDelta = 24241.118854\nsuperstep = 4\tsumDelta = 20605.006026\nsuperstep = 5\tsumDelta = 17514.126263\nsuperstep = 6\tsumDelta = 14887.007741\nsuperstep = 7\tsumDelta = 12653.956935\nsuperstep = 8\tsumDelta = 10755.863697\nsuperstep = 9\tsumDelta = 9142.484399\nsuperstep = 10\tsumDelta = 7771.111957\nsuperstep = 11\tsumDelta = 6605.445348\nsuperstep = 12\tsumDelta = 5614.628704\nsuperstep = 13\tsumDelta = 4772.434532\nsuperstep = 14\tsumDelta = 4056.569466\nsuperstep = 15\tsumDelta = 3448.084143\nsuperstep = 16\tsumDelta = 2930.871604\nsuperstep = 17\tsumDelta = 2491.240933\nsuperstep = 18\tsumDelta = 2117.554852\nsuperstep = 19\tsumDelta = 1799.921675\nsuperstep = 20\tsumDelta = 1529.933467\nsuperstep = 21\tsumDelta = 1300.443483\nsuperstep = 22\tsumDelta = 1105.376992\nsuperstep = 23\tsumDelta = 939.570469\nsuperstep = 24\tsumDelta = 798.634921\nsuperstep = 25\tsumDelta = 678.839702\nsuperstep = 26\tsumDelta = 577.013763\nsuperstep = 27\tsumDelta = 489.603196\nsuperstep = 28\tsumDelta = 415.350199\nsuperstep = 29\tsumDelta = 352.477966\nsuperstep = 30\tsumDelta = 299.025706'}
""")))

  override def execute(invocation: Invocation, arguments: Pr)(implicit user: UserPrincipal, executionContext: ExecutionContext): PrResult = {
    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")
    val titanConf = GiraphConfigurationUtil.flattenConfig(config.getConfig("titan"), "titan.")

    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "pr.maxSupersteps", arguments.max_supersteps)
    GiraphConfigurationUtil.set(hConf, "pr.convergenceThreshold", arguments.convergence_threshold)
    GiraphConfigurationUtil.set(hConf, "pr.resetProbability", arguments.reset_probability)
    GiraphConfigurationUtil.set(hConf, "pr.convergenceProgressOutputInterval", arguments.convergence_progress_output_interval)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, titanConf, graph)

    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", arguments.input_edge_label_list)
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", arguments.output_vertex_property_list)

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanHBaseVertexInputFormatLongDoubleNull])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatLongIDDoubleValue[_ <: org.apache.hadoop.io.LongWritable, _ <: org.apache.hadoop.io.DoubleWritable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[PageRankComputation.PageRankMasterCompute])
    giraphConf.setComputationClass(classOf[PageRankComputation])
    giraphConf.setAggregatorWriterClass(classOf[PageRankComputation.PageRankAggregatorWriter])

    PrResult(GiraphJobManager.run("ia_giraph_pr",
      classOf[PageRankComputation].getCanonicalName,
      config, giraphConf, invocation, "pr-convergence-report_0"))

  }

}
