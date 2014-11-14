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

import com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation
import com.intel.giraph.io.titan.formats.{ TitanVertexOutputFormatPropertyGraph4LBP, TitanVertexInputFormatPropertyGraph4LBP }
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

case class Lbp(graph: GraphReference,
               vertexValuePropertyList: List[String],
               edgeValuePropertyList: List[String],
               inputEdgeLabelList: List[String],
               outputVertexPropertyList: List[String],
               vertexType: String,
               vectorValue: Boolean,
               maxSupersteps: Option[Int] = None,
               convergenceThreshold: Option[Double] = None,
               anchorThreshold: Option[Double] = None,
               smoothing: Option[Double] = None,
               validateGraphStructure: Option[Boolean] = None,
               ignoreVertexType: Option[Boolean] = None,
               maxProduct: Option[Boolean] = None,
               power: Option[Double] = None)

case class LbpResult(value: String) //TODO

/** Json conversion for arguments and return value case classes */
object LbpJsonFormat {
  import DomainJsonProtocol._
  implicit val lbpFormat = jsonFormat15(Lbp)
  implicit val lbpResultFormat = jsonFormat1(LbpResult)
}

import LbpJsonFormat._

class LoopyBeliefPropagation
    extends CommandPlugin[Lbp, LbpResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:titan/ml/loopy_belief_propagation"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc = Some(CommandDoc(oneLineSummary = "Loopy belief propagation on Markov Random Fields(MRF).",
    extendedSummary = Some("""
                           |    This algorithm was originally designed for acyclic graphical models,
                           |    then it was found that the Belief Propagation algorithm can be used
                           |    in general graphs. The algorithm is then sometimes called "loopy"
                           |    belief propagation, because graphs typically contain cycles, or loops.
                           | 
                           |    In Giraph, we run the algorithm in iterations until it converges.
                           | 
                           |    Parameters
                           |    ----------
                           |    vertex_value_property_list : list of string
                           |        The vertex properties which contain prior vertex values if you
                           |        use more than one vertex property.
                           |
                           |    edge_value_property_list : list of string
                           |        The edge properties which contain the input edge values.
                           |        We expect comma-separated list of property names  if you use
                           |        more than one edge property.
                           |
                           |    input_edge_label_list : list of string
                           |        The name of edge label.
                           |
                           |    output_vertex_property_list : list of string
                           |        The list of vertex properties to store output vertex values.
                           |
                           |    vertex_type : string
                           |        The name of vertex property which contains vertex type.
                           |        The default value is "vertex_type"
                           |
                           |    vector_value : boolean
                           |        True means a vector as vertex value is supported
                           |        False means a vector as vertex value is not supported
                           |
                           |    max_supersteps : integer (optional)
                           |        The maximum number of super steps that the algorithm will execute.
                           |        The valid value range is all positive integer.
                           |        The default value is 20.
                           |
                           |    convergence_threshold : float (optional)
                           |        The amount of change in cost function that will be tolerated at
                           |        convergence.
                           |        If the change is less than this threshold, the algorithm exists earlier
                           |        before it reaches the maximum number of super steps.
                           |        The valid value range is all float and zero.
                           |        The default value is 0.001.
                           |
                           |    anchor_threshold : float (optional)
                           |        The parameter that determines if a node's posterior will be updated or
                           |        not.
                           |        If a node's maximum prior value is greater than this threshold, the node
                           |        will be treated as anchor node, whose posterior will inherit from prior
                           |        without update.
                           |        This is for the case where we have confident prior estimation for some
                           |        nodes and don't want the algorithm updates these nodes.
                           |        The valid value range is in [0, 1].
                           |        The default value is 1.0.
                           |
                           |    smoothing : float (optional)
                           |        The Ising smoothing parameter.
                           |        This parameter adjusts the relative strength of closeness encoded edge
                           |        weights, similar to the width of Gussian distribution.
                           |        Larger value implies smoother decay and the edge weight beomes less
                           |        important.
                           |        The default value is 2.0.
                           |
                           |    validate_graph_structure : boolean (optional)
                           |        Checks if the graph meets certain structural requirements before starting
                           |        the algorithm.
                           |
                           |        At present, this checks that at every vertex, the in-degree equals the
                           |        out-degree. Because this algorithm is for undirected graphs, this is a necessary
                           |        but not sufficient, check for valid input.
                           |
                           |    ignore_vertex_type : boolean (optional)
                           |        If true, all vertex will be treated as training data.
                           |        The default value is False.
                           |
                           |    max_product : boolean (optional)
                           |        Should LBP use max_product or not.
                           |        The default value is False.
                           |
                           |    power: float (optional)
                           |        Power coefficient for power edge potential.
                           |        The default value is 0.
                           | 
                           |    Returns
                           |    -------
                           |    Multiple line string
                           |        The configuration and learning curve report for Loopy Belief Propagation
                           | 
                           |    Examples
                           |    --------
                           |    ::
                           | 
                           |        g.ml.loopy_belief_propagation(vertex_value_property_list = "value", edge_value_property_list  = "weight", input_edge_label_list = "edge",   output_vertex_property_list = "lbp_posterior",   vertex_type_property_key = "vertex_type",  vector_value = "true",    max_supersteps = 10,   convergence_threshold = 0.0, anchor_threshold = 0.9, smoothing = 2.0, bidirectional_check = False,  ignore_vertex_type = False, max_product= False, power = 0)
                           | 
                           |    The expected output is like this::
                           | 
                           |        {u'value': u'======Graph Statistics======\\nNumber of vertices: 80000 (train: 56123, validate: 15930, test: 7947)\\nNumber of edges: 318400\\n\\n======LBP Configuration======\\nmaxSupersteps: 10\\nconvergenceThreshold: 0.000000\\nanchorThreshold: 0.900000\\nsmoothing: 2.000000\\nbidirectionalCheck: false\\nignoreVertexType: false\\nmaxProduct: false\\npower: 0.000000\\n\\n======Learning Progress======\\nsuperstep = 1\\tavgTrainDelta = 0.594534\\tavgValidateDelta = 0.542366\\tavgTestDelta = 0.542801\\nsuperstep = 2\\tavgTrainDelta = 0.322596\\tavgValidateDelta = 0.373647\\tavgTestDelta = 0.371556\\nsuperstep = 3\\tavgTrainDelta = 0.180468\\tavgValidateDelta = 0.194503\\tavgTestDelta = 0.198478\\nsuperstep = 4\\tavgTrainDelta = 0.113280\\tavgValidateDelta = 0.117436\\tavgTestDelta = 0.122555\\nsuperstep = 5\\tavgTrainDelta = 0.076510\\tavgValidateDelta = 0.074419\\tavgTestDelta = 0.077451\\nsuperstep = 6\\tavgTrainDelta = 0.051452\\tavgValidateDelta = 0.051683\\tavgTestDelta = 0.052538\\nsuperstep = 7\\tavgTrainDelta = 0.038257\\tavgValidateDelta = 0.033629\\tavgTestDelta = 0.034017\\nsuperstep = 8\\tavgTrainDelta = 0.027924\\tavgValidateDelta = 0.026722\\tavgTestDelta = 0.025877\\nsuperstep = 9\\tavgTrainDelta = 0.022886\\tavgValidateDelta = 0.019267\\tavgTestDelta = 0.018190\\nsuperstep = 10\\tavgTrainDelta = 0.018271\\tavgValidateDelta = 0.015924\\tavgTestDelta = 0.015377'}
                           | 
                            """.stripMargin)))

  override def execute(invocation: Invocation, arguments: Lbp)(implicit user: UserPrincipal, executionContext: ExecutionContext): LbpResult = {

    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")

    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "lbp.maxSupersteps", arguments.maxSupersteps)
    GiraphConfigurationUtil.set(hConf, "lbp.convergenceThreshold", arguments.convergenceThreshold)
    GiraphConfigurationUtil.set(hConf, "lbp.anchorThreshold", arguments.anchorThreshold)
    GiraphConfigurationUtil.set(hConf, "lbp.bidirectionalCheck", arguments.validateGraphStructure)
    GiraphConfigurationUtil.set(hConf, "lbp.power", arguments.power)
    GiraphConfigurationUtil.set(hConf, "lbp.smoothing", arguments.smoothing)
    GiraphConfigurationUtil.set(hConf, "lbp.ignoreVertexType", arguments.ignoreVertexType)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, config, graph)

    GiraphConfigurationUtil.set(hConf, "input.vertex.value.property.key.list", Some(arguments.vertexValuePropertyList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "input.edge.value.property.key.list", Some(arguments.edgeValuePropertyList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", Some(arguments.inputEdgeLabelList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", Some(arguments.outputVertexPropertyList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "vertex.type.property.key", Some(arguments.vertexType))
    GiraphConfigurationUtil.set(hConf, "vector.value", Some(arguments.vectorValue.toString))

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanVertexInputFormatPropertyGraph4LBP])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4LBP[_ <: org.apache.hadoop.io.WritableComparable[_], _ <: org.apache.hadoop.io.Writable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[LoopyBeliefPropagationComputation.LoopyBeliefPropagationMasterCompute])
    giraphConf.setComputationClass(classOf[LoopyBeliefPropagationComputation])
    giraphConf.setAggregatorWriterClass(classOf[LoopyBeliefPropagationComputation.LoopyBeliefPropagationAggregatorWriter])

    LbpResult(GiraphJobManager.run("ia_giraph_lbp",
      classOf[LoopyBeliefPropagationComputation].getCanonicalName,
      config, giraphConf, invocation, "lbp-learning-report_0"))
  }

}
