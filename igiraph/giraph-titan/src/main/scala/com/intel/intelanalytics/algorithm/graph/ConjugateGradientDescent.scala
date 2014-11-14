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

import com.intel.giraph.algorithms.cgd.ConjugateGradientDescentComputation
import com.intel.giraph.io.titan.formats.{ TitanVertexOutputFormatPropertyGraph4CF, TitanVertexInputFormatPropertyGraph4CFCGD, TitanVertexInputFormatPropertyGraph4CF }
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
import scala.collection.JavaConverters._
import com.intel.intelanalytics.domain.command.CommandDoc

case class Cgd(graph: GraphReference,
               edgeValuePropertyList: List[String],
               inputEdgeLabelList: List[String],
               outputVertexPropertyList: List[String],
               vertexTypePropertyKey: String,
               edgeTypePropertyKey: String,
               vectorValue: Option[Boolean] = None,
               maxSupersteps: Option[Int] = None,
               convergenceThreshold: Option[Double] = None,
               cgdLambda: Option[Float] = None,
               featureDimension: Option[Int] = None,
               learningCurveOutputInterval: Option[Int] = None,
               validateGraphStructure: Option[Boolean] = None,
               biasOn: Option[Boolean] = None,
               maxValue: Option[Float] = None,
               minValue: Option[Float] = None,
               numIters: Option[Int] = None)

case class CgdResult(value: String)

/** Json conversion for arguments and return value case classes */
object CgdJsonFormat {
  import DomainJsonProtocol._
  implicit val cgdFormat = jsonFormat17(Cgd)
  implicit val cgdResultFormat = jsonFormat1(CgdResult)
}

import CgdJsonFormat._

class ConjugateGradientDescent
    extends CommandPlugin[Cgd, CgdResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:titan/ml/conjugate_gradient_descent"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc = Some(CommandDoc(oneLineSummary = "The Conjugate Gradient Descent (CGD) with Bias for collaborative filtering algorithms.",
    extendedSummary = Some("""
                           |    Extended Summary
                           |    ----------------
                           |    CGD implementation of the algorithm presented in Y. Koren.
                           |    Factorization Meets the Neighborhood: a Multifaceted
                           |    Collaborative Filtering Model.
                           |    In ACM KDD 2008. (Equation 5)
                           | 
                           |    Parameters
                           |    ----------
                           |    edge_value_property_list : list of string
                           |        The edge properties which contain the input edge values.
                           |        A comma-separated list of property names when declaring
                           |        more than one edge property.
                           | 
                           |    input_edge_label_list : list of string
                           |        The name of edge label
                           | 
                           |    output_vertex_property_list : list of string  
                           |        The list of vertex properties to store output vertex values
                           | 
                           |    vertex_type_property_key : string
                           |        The name of vertex property which contains vertex type
                           | 
                           |    edge_type_property_key : string
                           |        The name of edge property which contains edge type
                           | 
                           |    vector_value: string (optional)
                           |        True means a vector as vertex value is supported,
                           |        False means a vector as vertex value is not supported.
                           |        The default value is False.
                           | 
                           |    max_supersteps : integer (optional)
                           |        The maximum number of super steps (iterations) that the algorithm
                           |        will execute.  The default value is 20.
                           | 
                           |    convergence_threshold : float (optional)
                           |        The amount of change in cost function that will be tolerated at
                           |        convergence.
                           |        If the change is less than this threshold, the algorithm exists earlier
                           |        before it reaches the maximum number of super steps.
                           |        The valid value range is all float and zero.
                           |        The default value is 0.
                           | 
                           |    cgd_lambda : float (optional)
                           |        The tradeoff parameter that controls the strength of regularization.
                           |        Larger value implies stronger regularization that helps prevent
                           |        overfitting but may cause the issue of underfitting if the value is too
                           |        large.
                           |        The value is usually determined by cross validation (CV).
                           |        The valid value range is all positive float and zero.
                           |        The default value is 0.065.
                           | 
                           |    feature_dimension : integer (optional)
                           |        The length of feature vector to use in CGD model.
                           |        Larger value in general results in more accurate parameter estimation,
                           |        but slows down the computation.
                           |        The valid value range is all positive integer.
                           |        The default value is 3.
                           | 
                           |    learning_curve_output_interval : integer (optional)
                           |        The learning curve output interval.
                           |        Since each CGD iteration is composed by 2 super steps,
                           |        the default one (1) iteration means two super steps.
                           |
                           |    validate_graph_structure : boolean (optional)
                           |        Checks if the graph meets certain structural requirements before starting
                           |        the algorithm.
                           |
                           |        At present, this checks that at every vertex, the in-degree equals the
                           |        out-degree. Because this algorithm is for undirected graphs, this is a necessary
                           |        but not sufficient, check for valid input.
                           | 
                           |    bias_on : boolean (optional)
                           |        True means turn on the update for bias term and False means turn off
                           |        the update for bias term.
                           |        Turning it on often yields more accurate model with minor performance
                           |        penalty; turning it off disables term update and leaves the value of
                           |        bias term to be zero.
                           |        The default value is false.
                           | 
                           |    max_value : float (optional)
                           |        The maximum edge weight value. If an edge weight is larger than this
                           |        value, the algorithm will throw an exception and terminate. This option
                           |        is mainly for graph integrity check.
                           |        Valid value range is all Float.
                           |        The default value is "Infinity".
                           | 
                           |    min_value : float (optional)
                           |        The minimum edge weight value. If an edge weight is smaller than this
                           |        value, the algorithm will throw an exception and terminate. This option
                           |        is mainly for graph integrity check.
                           |        Valid value range is all Float.
                           |        The default value is "-Infinity".
                           |
                           |    Notes
                           |    -----
                           |    Vertices must be identified as left-side ("L") or right-side ("R").
                           |    See vertex rules.
                           | 
                           |    Returns
                           |    -------
                           |    Multiple line string
                           |        The configuration and learning curve report for CGD.
                           | 
                           |    Examples
                           |    --------
                           |    ::
                           |        g.ml.conjugate_gradient_descent(edge_value_property_list = "rating", vertex_type_property_key = "vertex_type", input_edge_label_list = "edge", output_vertex_property_list = "cgd_result", edge_type_property_key = "splits", vector_value = "true", cgd_lambda = 0.065, num_iters = 3)
                           | 
                           |    The expected output is like this::
                           | 
                           |        {u'value': u'======Graph Statistics======\\nNumber of vertices: 20140 (left: 10070, right: 10070)\\nNumber of edges: 604016 (train: 554592, validate: 49416, test: 8)\\n\\n======CGD Configuration======\\nmaxSupersteps: 20\\nfeatureDimension: 3\\nlambda: 0.065000\\nbiasOn: false\\nconvergenceThreshold: 0.000000\\nbidirectionalCheck: false\\nnumCGDIters: 3\\nmaxVal: Infinity\\nminVal: -Infinity\\nlearningCurveOutputInterval: 1\\n\\n======Learning Progress======\\nsuperstep = 2\\tcost(train) = 21828.395401\\trmse(validate) = 1.317799\\trmse(test) = 3.663107\\nsuperstep = 4\\tcost(train) = 18126.623261\\trmse(validate) = 1.247019\\trmse(test) = 3.565567\\nsuperstep = 6\\tcost(train) = 15902.042769\\trmse(validate) = 1.209014\\trmse(test) = 3.677774\\nsuperstep = 8\\tcost(train) = 14274.718100\\trmse(validate) = 1.196888\\trmse(test) = 3.656467\\nsuperstep = 10\\tcost(train) = 13226.419606\\trmse(validate) = 1.189605\\trmse(test) = 3.699198\\nsuperstep = 12\\tcost(train) = 12438.789925\\trmse(validate) = 1.187416\\trmse(test) = 3.653920\\nsuperstep = 14\\tcost(train) = 11791.454643\\trmse(validate) = 1.188480\\trmse(test) = 3.670579\\nsuperstep = 16\\tcost(train) = 11256.035422\\trmse(validate) = 1.187924\\trmse(test) = 3.742146\\nsuperstep = 18\\tcost(train) = 10758.691712\\trmse(validate) = 1.189491\\trmse(test) = 3.658956\\nsuperstep = 20\\tcost(train) = 10331.742207\\trmse(validate) = 1.191606\\trmse(test) = 3.757683'}
                           | 
                            """.stripMargin)))

  override def execute(arguments: Cgd)(implicit context: Invocation): CgdResult = {

    val config = configuration
    val pattern = "[\\s,\\t]+"
    val outputVertexPropertyList = arguments.outputVertexPropertyList.mkString(",")
    val resultPropertyList = outputVertexPropertyList.split(pattern)
    val vectorValue = arguments.vectorValue.getOrElse(false)
    val biasOn = arguments.biasOn.getOrElse(false)
    require(resultPropertyList.size >= 1,
      "Please input at least one vertex property name for ALS/CGD results")
    require(!vectorValue || !biasOn ||
      (vectorValue && biasOn && resultPropertyList.size == 2),
      "Please input one property name for bias and one property name for results when both vector_value " +
        "and bias_on are enabled")
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")

    val graphFuture = context.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)
    val biasOnOption = if (biasOn) Option(biasOn.toString().toLowerCase()) else None

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "cgd.maxSupersteps", arguments.maxSupersteps)
    GiraphConfigurationUtil.set(hConf, "cgd.convergenceThreshold", arguments.convergenceThreshold)
    GiraphConfigurationUtil.set(hConf, "cgd.featureDimension", arguments.featureDimension)
    GiraphConfigurationUtil.set(hConf, "cgd.bidirectionalCheck", arguments.validateGraphStructure)
    GiraphConfigurationUtil.set(hConf, "cgd.biasOn", arguments.biasOn)
    GiraphConfigurationUtil.set(hConf, "cgd.lambda", arguments.cgdLambda)
    GiraphConfigurationUtil.set(hConf, "cgd.learningCurveOutputInterval", arguments.learningCurveOutputInterval)
    GiraphConfigurationUtil.set(hConf, "cgd.maxVal", arguments.maxValue)
    GiraphConfigurationUtil.set(hConf, "cgd.minVal", arguments.minValue)
    GiraphConfigurationUtil.set(hConf, "cgd.numCGDIters", arguments.numIters)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, config, graph)

    GiraphConfigurationUtil.set(hConf, "input.edge.value.property.key.list", Some(arguments.edgeValuePropertyList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", Some(arguments.inputEdgeLabelList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", Some(arguments.outputVertexPropertyList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "vertex.type.property.key", Some(arguments.vertexTypePropertyKey))
    GiraphConfigurationUtil.set(hConf, "edge.type.property.key", Some(arguments.edgeTypePropertyKey))
    GiraphConfigurationUtil.set(hConf, "vector.value", Some(vectorValue.toString))
    GiraphConfigurationUtil.set(hConf, "output.vertex.bias", Some(biasOn))

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanVertexInputFormatPropertyGraph4CFCGD])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4CF[_ <: org.apache.hadoop.io.WritableComparable[_], _ <: org.apache.hadoop.io.Writable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[ConjugateGradientDescentComputation.ConjugateGradientDescentMasterCompute])
    giraphConf.setComputationClass(classOf[ConjugateGradientDescentComputation])
    giraphConf.setAggregatorWriterClass(classOf[ConjugateGradientDescentComputation.ConjugateGradientDescentAggregatorWriter])

    CgdResult(GiraphJobManager.run("ia_giraph_cgd",
      classOf[ConjugateGradientDescentComputation].getCanonicalName,
      config, giraphConf, context, "cgd-learning-report_0"))
  }

}
