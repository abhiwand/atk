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

import com.intel.giraph.algorithms.lp.LabelPropagationComputation
import com.intel.giraph.io.titan.formats.{ TitanVertexOutputFormatPropertyGraph4LP, TitanVertexInputFormatPropertyGraph4LP }
import com.intel.intelanalytics.algorithm.util.{ GiraphJobManager, GiraphConfigurationUtil }
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.giraph.conf.GiraphConfiguration
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent.duration._

import scala.concurrent._
import com.intel.intelanalytics.domain.command.CommandDoc

case class Lp(graph: GraphReference,
              vertex_value_property_list: List[String],
              edge_value_property_list: List[String],
              input_edge_label_list: List[String],
              output_vertex_property_list: List[String],
              vector_value: Boolean,
              max_supersteps: Option[Int] = None,
              convergence_threshold: Option[Double] = None,
              anchor_threshold: Option[Double] = None,
              lp_lambda: Option[Double] = None,
              bidirectional_check: Option[Boolean] = None)
case class LpResult(value: String) //TODO

/** Json conversion for arguments and return value case classes */
object LpJsonFormat {
  import DomainJsonProtocol._
  implicit val lbpFormat = jsonFormat11(Lp)
  implicit val lbpResultFormat = jsonFormat1(LpResult)
}

import LpJsonFormat._

class LabelPropagation
    extends CommandPlugin[Lp, LpResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:titan/ml/label_propagation"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc = Some(CommandDoc(oneLineSummary = "Label Propagation on Gaussian Random Fields.",
    extendedSummary = Some("""
                           |    This algorithm is presented in X. Zhu and Z. Ghahramani.
                           |    Learning from labeled and unlabeled data with label propagation.
                           |    Technical Report CMU-CALD-02-107, CMU, 2002.
                           | 
                           |    Parameters
                           |    ----------
                           |    vertex_value_property_list : list of string
                           |        The vertex properties which contain prior vertex values if you use more
                           |        than one vertex property.
                           |    edge_value_property_list : list of string
                           |        The edge properties which contain the input edge values.
                           |        We expect comma-separated list of property names if you use more than
                           |        one edge property.
                           |    input_edge_label_list : list of string
                           |        The name of edge label
                           |    output_vertex_property_list : list of string
                           |        The list of vertex properties to store output vertex values
                           |    vector_value : boolean
                           |        True means a vector as vertex value is supported,
                           |        False means a vector as vertex value is not supported
                           |    max_supersteps : integer (optional)
                           |        The maximum number of super steps that the algorithm will execute.
                           |        The valid value range is all positive integer.
                           |        The default value is 10.
                           |    convergence_threshold : float (optional)
                           |        The amount of change in cost function that will be tolerated at
                           |        convergence.
                           |        If the change is less than this threshold, the algorithm exists earlier
                           |        before it reaches the maximum number of super steps.
                           |        The valid value range is all float and zero.
                           |        The default value is 0.001.
                           |    anchor_threshold : float (optional)
                           |        The parameter that determines if a node's initial prediction from
                           |        external classifier will be updated or not.
                           |        If a node's maximum initial prediction value is greater than this
                           |        threshold, the node will be treated as anchor node, whose final
                           |        prediction will inherit from prior without update.
                           |        This is for the case where we have confident initial predictions on some
                           |        nodes and don't want the algorithm updates those nodes.
                           |        The valid value range is [0, 1].
                           |        The default value is 1.0
                           |    lp_lambda : float (optional)
                           |        The tradeoff parameter that controls much influence of external
                           |        classifier's prediction contribution to the final prediction.
                           |        This is for the case where an external classifier is available that can
                           |        produce initial probabilistic classification on unlabled examples, and
                           |        the option allows incorporating external classifier's prediction into
                           |        the LP training process.
                           |        The valid value range is [0.0,1.0].
                           |        The default value is 0.
                           |    bidirectional_check : boolean (optional)
                           |        If it is true, Giraph will firstly check whether each edge is
                           |        bidirectional before running algorithm.
                           |        LP expects an undirected input graph and each edge therefore should be
                           |        bi-directional.
                           |        This option is mainly for graph integrity check.
                           | 
                           |    Returns
                           |    -------
                           |    Multiple line string
                           |        The configuration and learning curve report for Label Propagation
                           | 
                           |    Examples
                           |    --------
                           |    ::
                           | 
                           |        g.ml.label_propagation(vertex_value_property_list = "input_value", edge_value_property_list  = "weight", input_edge_label_list = "edge",   output_vertex_property_list = "lp_posterior",   vector_value = "true",    max_supersteps = 10,   convergence_threshold = 0.0, anchor_threshold = 0.9, lp_lambda = 0.5, bidirectional_check = False)
                           | 
                           |    The expected output is like this::
                           | 
                           |        {u'value': u'======Graph Statistics======\\nNumber of vertices: 600\\nNumber of edges: 15716\\n\\n======LP Configuration======\\nlambda: 0.000000\\nanchorThreshold: 0.900000\\nconvergenceThreshold: 0.000000\\nmaxSupersteps: 10\\nbidirectionalCheck: false\\n\\n======Learning Progress======\\nsuperstep = 1\\tcost = 0.008692\\nsuperstep = 2\\tcost = 0.008155\\nsuperstep = 3\\tcost = 0.007809\\nsuperstep = 4\\tcost = 0.007544\\nsuperstep = 5\\tcost = 0.007328\\nsuperstep = 6\\tcost = 0.007142\\nsuperstep = 7\\tcost = 0.006979\\nsuperstep = 8\\tcost = 0.006833\\nsuperstep = 9\\tcost = 0.006701\\nsuperstep = 10\\tcost = 0.006580'}
                           | 
                            """.stripMargin)))

  override def execute(arguments: Lp)(implicit context: Invocation): LpResult = {

    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")

    val graphFuture = context.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "lp.maxSupersteps", arguments.max_supersteps)
    GiraphConfigurationUtil.set(hConf, "lp.convergenceThreshold", arguments.convergence_threshold)
    GiraphConfigurationUtil.set(hConf, "lp.anchorThreshold", arguments.anchor_threshold)
    GiraphConfigurationUtil.set(hConf, "lp.bidirectionalCheck", arguments.bidirectional_check)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, config, graph)

    GiraphConfigurationUtil.set(hConf, "input.vertex.value.property.key.list", Some(arguments.vertex_value_property_list.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "input.edge.value.property.key.list", Some(arguments.edge_value_property_list.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", Some(arguments.input_edge_label_list.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", Some(arguments.output_vertex_property_list.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "vector.value", Some(arguments.vector_value.toString))

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanVertexInputFormatPropertyGraph4LP])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4LP[_ <: org.apache.hadoop.io.LongWritable, _ <: com.intel.giraph.io.VertexData4LPWritable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[LabelPropagationComputation.LabelPropagationMasterCompute])
    giraphConf.setComputationClass(classOf[LabelPropagationComputation])
    giraphConf.setAggregatorWriterClass(classOf[LabelPropagationComputation.LabelPropagationAggregatorWriter])

    LpResult(GiraphJobManager.run("ia_giraph_lp",
      classOf[LabelPropagationComputation].getCanonicalName,
      config, giraphConf, context, "lp-learning-report_0"))
  }

}
