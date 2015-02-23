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

//
//package com.intel.intelanalytics.algorithm.graph
//
//import com.intel.giraph.algorithms.gbp.GaussianBeliefPropagationComputation
//import com.intel.giraph.io.formats.{ JsonPropertyGraph4GBPOutputFormat, JsonPropertyGraph4GBPInputFormat }
//import com.intel.giraph.io.titan.formats.{ TitanVertexOutputFormatPropertyGraph4LBP, TitanVertexInputFormatPropertyGraph4LBP }
//import com.intel.intelanalytics.algorithm.util.{ GiraphJobManager, GiraphConfigurationUtil }
//import com.intel.intelanalytics.domain.DomainJsonProtocol
//import com.intel.intelanalytics.domain.DomainJsonProtocol._
//import com.intel.intelanalytics.domain.command.CommandDoc
//import com.intel.intelanalytics.domain.graph.GraphReference
//import com.intel.intelanalytics.engine.plugin.{ Invocation, CommandPlugin }
//import com.intel.intelanalytics.security.UserPrincipal
//import org.apache.giraph.conf.GiraphConfiguration
//import scala.concurrent.duration._
//import spray.json.DefaultJsonProtocol._
//import spray.json._
//
//import scala.concurrent.{ Await, ExecutionContext }
//
//case class Gbp(graph: GraphReference,
//               vertexValuePropertyList: List[String],
//               edgeValuePropertyList: List[String],
//               inputEdgeLabelList: List[String],
//               outputVertexPropertyList: List[String],
//               maxSupersteps: Option[Int] = None,
//               convergenceThreshold: Option[Double] = None,
//               bidirectionalCheck: Option[Boolean] = None,
//               outerLoop: Option[Boolean] = None)
//
//case class GbpResult(value: String)
//
//object GbpJsonFormat {
//  import DomainJsonProtocol._
//  implicit val gbpFormat = jsonFormat9(Gbp)
//  implicit val gbpResultFormat = jsonFormat1(GbpResult)
//}
//
//import GbpJsonFormat._
//
//class GaussianBeliefPropagation
//    extends CommandPlugin[Gbp, GbpResult] {
//
//  /**
//   * The name of the command, e.g. graphs/ml/gaussian_belief_propagation
//   *
//   * The format of the name determines how the plugin gets "installed" in the client layer
//   * e.g Python client via code generation.
//   */
//  override def name: String = "graph:titan/ml/gaussian_belief_propagation"
//
//  /**
//   * User documentation exposed in Python.
//   *
//   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
//   */
//  override def doc = Some(CommandDoc(oneLineSummary = "Gaussian Belief Propagation",
//    extendedSummary = Some(
//      """
//                              |Parameters
//                              |    ----------
//                              |    vertex_prior_mean_property_name : string
//                              |        The vertex property which contains prior vertex mea
//                              |
//                              |    vertex_prior_precision_property_name: string
//                              |        The vertex property which contains prioer vertex pr
//                              |
//                              |    input_edge_label_list : list of string
//                              |        The name of edge label.
//                              |
//                              |    output_vertex_property_list : list of string
//                              |        The list of vertex properties to store output vertex values.
//                              |
//                              |    max_supersteps : integer (optional)
//                              |        The maximum number of super steps that the algorithm will execute.
//                              |        The valid value range is all positive integer.
//                              |        The default value is 20.
//                              |
//                              |    convergence_threshold : float (optional)
//                              |        The amount of change in cost function that will be tolerated at
//                              |        convergence.
//                              |        If the change is less than this threshold, the algorithm exists earlier
//                              |        before it reaches the maximum number of super steps.
//                              |        The valid value range is all float and zero.
//                              |        The default value is 0.001.
//                              |
//                              |    bidirectionalCheck: Boolean (optional)
//                              |        Checks if the graph meets certain structural requirements before starting
//                              |        the algorithm.
//                              |
//                              |        At present, this checks that at every vertex, the in-degree equals the
//                              |        out-degree. Because this algorithm is for undirected graphs, this is a necessary
//                              |        but not sufficient, check for valid input.
//                              |
//                              |        The default value is false
//                              |
//                              |    outerLoop: Boolean (optional)
//                              |         The default value is true
//                              |
//                              |    Returns
//                              |    -------
//                              |    Multiple line string
//                              |        The configuration and learning curve report for Gaussian Belief Propagation
//                              |
//                              |    Examples
//                              |    --------
//                              |
//  """.stripMargin)))
//
//  //override def execute(invocation: Invocation, arguments: Gbp)(implicit user: UserPrincipal, executionContext: ExecutionContext): GbpResult = {
//
//    override def execute(arguments: Gbp)(implicit invocation: Invocation): GbpResult = {
//
//    val config = configuration
//    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")
//
//    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
//    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)
//
//    GiraphConfigurationUtil.set(hConf, "gbp.maxSupersteps", arguments.maxSupersteps)
//    GiraphConfigurationUtil.set(hConf, "gbp.convergenceThreshold", arguments.convergenceThreshold)
//    GiraphConfigurationUtil.set(hConf, "gbp.bidirectionalCheck", arguments.bidirectionalCheck)
//    GiraphConfigurationUtil.set(hConf, "gbp.outerLoop", arguments.outerLoop)
//
//    GiraphConfigurationUtil.initializeTitanConfig(hConf, config, graph)
//
//    GiraphConfigurationUtil.set(hConf, "input.vertex.value.property.key.list", Some(arguments.vertexValuePropertyList.mkString(",")))
//    GiraphConfigurationUtil.set(hConf, "input.edge.value.property.key.list", Some(arguments.edgeValuePropertyList.mkString(",")))
//    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", Some(arguments.inputEdgeLabelList.mkString(",")))
//    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", Some(arguments.outputVertexPropertyList.mkString(",")))
//
//    val giraphConf = new GiraphConfiguration(hConf)
//
//    giraphConf.setVertexInputFormatClass(classOf[TitanVertexInputFormatPropertyGraph4LBP])
//    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4LBP[_ <: org.apache.hadoop.io.WritableComparable[_], _ <: org.apache.hadoop.io.Writable, _ <: org.apache.hadoop.io.Writable]])
//    giraphConf.setMasterComputeClass(classOf[GaussianBeliefPropagationComputation.GaussianBeliefPropagationMasterCompute])
//    giraphConf.setComputationClass(classOf[GaussianBeliefPropagationComputation])
//    giraphConf.setAggregatorWriterClass(classOf[GaussianBeliefPropagationComputation.GaussianBeliefPropagationAggregatorWriter])
//
//    GbpResult(GiraphJobManager.run("ia_giraph_gbp",
//      classOf[GaussianBeliefPropagationComputation].getCanonicalName,
//      config, giraphConf, invocation, "gbp-learning-report_0"))
//  }
//}
