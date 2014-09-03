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

import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4CF
import com.intel.giraph.io.titan.TitanVertexOutputFormatPropertyGraph4CF
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

case class Als(graph: GraphReference,
               edge_value_property_list: Option[String],
               input_edge_label_list: Option[String],
               output_vertex_property_list: Option[String],
               vertex_type_property_key: Option[String],
               edge_type_property_key: Option[String],
               vector_value: Option[String],
               max_supersteps: Option[Int] = None,
               convergence_threshold: Option[Double] = None,
               als_lambda: Option[Float] = None,
               feature_dimension: Option[Int] = None,
               learning_curve_output_interval: Option[Int] = None,
               bidirectional_check: Option[Boolean] = None,
               bias_on: Option[Boolean] = None,
               max_value: Option[Float] = None,
               min_value: Option[Float] = None)

case class AlsResult(value: String)

class AlternatingLeastSquares
    extends CommandPlugin[Als, AlsResult] {
  import DomainJsonProtocol._
  implicit val alsFormat = jsonFormat16(Als)
  implicit val alsResultFormat = jsonFormat1(AlsResult)

  override def doc = Some(CommandDoc(oneLineSummary = "The Alternating Least Squares with Bias for collaborative filtering algorithms.",
    extendedSummary = Some("""

    Extended Summary
    ----------------
    The algorithms presented in

    1. Y. Zhou, D. Wilkinson, R. Schreiber and R. Pan. Large-Scale
           Parallel Collaborative Filtering for the Netflix Prize. 2008.
    2. Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative
           Filtering Model. In ACM KDD 2008. (Equation 5)


    Parameters
    ----------
    edge_value_property_list : Comma Separated String
        The edge properties which contain the input edge values.
        We expect comma-separated list of property names  if you use
        more than one edge property.

    input_edge_label_list : Comma Separated String
        Name of edge label

    output_vertex_property_list : Comma Separated String
        The list of vertex properties to store output vertex values.

    vertex_type_property_key : String
        The name of vertex property which contains vertex type.

    edge_type_property_key : String
        The name of edge property which contains edge type.

    vector_value: String (optional)
        True means a vector as vertex value is supported
        False means a vector as vertex value is not supported
        The default value is False.

    max_supersteps : Integer, optional
        The maximum number of super steps (iterations) that the algorithm will execute.
        The default value is 20.

    convergence_threshold : Float (optional)
        The amount of change in cost function that will be tolerated at convergence.
        If the change is less than this threshold, the algorithm exists earlier
        before it reaches the maximum number of super steps.
        The valid value range is all Float and zero.
        The default value is 0.

    als_lambda : Float (optional)
        The tradeoff parameter that controls the strength of regularization.
        Larger value implies stronger regularization that helps prevent overfitting
        but may cause the issue of underfitting if the value is too large.
        The value is usually determined by cross validation (CV).
        The valid value range is all positive Float and zero.
        The default value is 0.065.

    feature_dimension : Integer, optional
        The length of feature vector to use in ALS model.
        Larger value in general results in more accurate parameter estimation,
        but slows down the computation.
        The valid value range is all positive integer.
        The default value is 3.

    learning_curve_output_interval : Integer (optional)
        The learning curve output interval.
        Since each ALS iteration is composed by 2 super steps,
        the default one iteration means two super steps.

    bidirectional_check : Boolean (optional)
        If it is True, Giraph will firstly check whether each edge is bidirectional
        before executing algorithm. ALS expects a bi-partite input graph and each edge
        therefore should be bi-directional. This option is mainly for graph integrity check.

    bias_on : Boolean (optional)
        True means turn on the update for bias term and False means turn off
        the update for bias term. Turning it on often yields more accurate model with
        minor performance penalty; turning it off disables term update and leaves the
        value of bias term to be zero.
        The default value is False.

    max_value : Float (optional)
        The maximum edge weight value. If an edge weight is larger than this
        value, the algorithm will throw an exception and terminate. This option
        is mainly for graph integrity check.
        Valid value range is all Float.
        The default value is Infinity.

    min_value : Float (optional)
        The minimum edge weight value. If an edge weight is smaller than this
        value, the algorithm will throw an exception and terminate. This option
        is mainly for graph integrity check.
        Valid value range is all Float.
        The default value is -Infinity.

    Raises
    ------
    ValueError
  	    When both bias_on and vector_value are true, we expect output_vertex_property_list contains
        two keys, one for output results, one for bias.

    Returns
    -------
    Multiple line string
  	    The configuration and learning curve report for ALS.

    Examples
    --------
    For example, if your left-side vertices are users, and you want to get movie recommendation for user 1,
    the commend to use are:


	g.ml.alternating_least_squares(edge_value_property_list = "rating", vertex_type_property_key = "vertex_type", input_edge_label_list = "edge", output_vertex_property_list = "als_result", edge_type_property_key = "splits", vector_value = "true", als_lambda = 0.065, bias_on = False, min_value = 1, max_value = 5)::


The expected output is like this


	{u'value': u'======Graph Statistics======\nNumber of vertices: 10070 (left: 9569, right: 501)\nNumber of edges: 302008 (train: 145182, validate: 96640, test: 60186)\n\n======ALS Configuration======\nmaxSupersteps: 20\nfeatureDimension: 3\nlambda: 0.065000\nbiasOn: False\nconvergenceThreshold: 0.000000\nbidirectionalCheck: False\nmaxVal: 5.000000\nminVal: 1.000000\nlearningCurveOutputInterval: 1\n\n======Learning Progress======\nsuperstep = 2\tcost(train) = 838.720244\trmse(validate) = 1.220795\trmse(test) = 1.226830\nsuperstep = 4\tcost(train) = 608.088979\trmse(validate) = 1.174247\trmse(test) = 1.180558\nsuperstep = 6\tcost(train) = 540.071050\trmse(validate) = 1.166471\trmse(test) = 1.172131\nsuperstep = 8\tcost(train) = 499.134869\trmse(validate) = 1.164236\trmse(test) = 1.169805\nsuperstep = 10\tcost(train) = 471.318913\trmse(validate) = 1.163796\trmse(test) = 1.169215\nsuperstep = 12\tcost(train) = 450.420300\trmse(validate) = 1.163993\trmse(test) = 1.169224\nsuperstep = 14\tcost(train) = 433.511180\trmse(validate) = 1.164485\trmse(test) = 1.169393\nsuperstep = 16\tcost(train) = 419.403410\trmse(validate) = 1.165008\trmse(test) = 1.169507\nsuperstep = 18\tcost(train) = 407.212140\trmse(validate) = 1.165425\trmse(test) = 1.169503\nsuperstep = 20\tcost(train) = 396.281966\trmse(validate) = 1.165723\trmse(test) = 1.169451'}::
""")))

  override def execute(invocation: Invocation, arguments: Als)(implicit user: UserPrincipal, executionContext: ExecutionContext): AlsResult = {

    val config = configuration
    val pattern = "[\\s,\\t]+"
    val outputVertexPropertyList = arguments.output_vertex_property_list.getOrElse(
      config.getString("output_vertex_property_list"))
    val resultPropertyList = outputVertexPropertyList.split(pattern)
    val vectorValue = arguments.vector_value.getOrElse(config.getString("vector_value")).toBoolean
    val biasOn = arguments.bias_on.getOrElse(false)
    require(resultPropertyList.size >= 1,
      "Please input at least one vertex property name for ALS/CGD results")
    require(!vectorValue || !biasOn ||
      (vectorValue && biasOn && resultPropertyList.size == 2),
      "Please input one property name for bias and one property name for results when both vector_value " +
        "and bias_on are enabled")
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")
    val titanConf = GiraphConfigurationUtil.flattenConfig(config.getConfig("titan"), "titan.")

    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)
    val biasOnOption = if (biasOn) Option(biasOn.toString().toLowerCase()) else None

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "als.maxSupersteps", arguments.max_supersteps)
    GiraphConfigurationUtil.set(hConf, "als.convergenceThreshold", arguments.convergence_threshold)
    GiraphConfigurationUtil.set(hConf, "als.featureDimension", arguments.feature_dimension)
    GiraphConfigurationUtil.set(hConf, "als.bidirectionalCheck", arguments.bidirectional_check)
    GiraphConfigurationUtil.set(hConf, "als.biasOn", arguments.bias_on)
    GiraphConfigurationUtil.set(hConf, "als.lambda", arguments.als_lambda)
    GiraphConfigurationUtil.set(hConf, "als.learningCurveOutputInterval", arguments.learning_curve_output_interval)
    GiraphConfigurationUtil.set(hConf, "als.maxVal", arguments.max_value)
    GiraphConfigurationUtil.set(hConf, "als.minVal", arguments.min_value)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, titanConf, graph)

    GiraphConfigurationUtil.set(hConf, "input.edge.value.property.key.list", arguments.edge_value_property_list)
    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", arguments.input_edge_label_list)
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", arguments.output_vertex_property_list)
    GiraphConfigurationUtil.set(hConf, "vertex.type.property.key", arguments.vertex_type_property_key)
    GiraphConfigurationUtil.set(hConf, "edge.type.property.key", arguments.edge_type_property_key)
    GiraphConfigurationUtil.set(hConf, "vector.value", arguments.vector_value)
    GiraphConfigurationUtil.set(hConf, "output.vertex.bias", biasOnOption)

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanHBaseVertexInputFormatPropertyGraph4CF])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4CF[_ <: org.apache.hadoop.io.WritableComparable[_], _ <: org.apache.hadoop.io.Writable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[AlternatingLeastSquaresComputation.AlternatingLeastSquaresMasterCompute])
    giraphConf.setComputationClass(classOf[AlternatingLeastSquaresComputation])
    giraphConf.setAggregatorWriterClass(classOf[AlternatingLeastSquaresComputation.AlternatingLeastSquaresAggregatorWriter])

    AlsResult(GiraphJobManager.run("ia_giraph_als",
      classOf[AlternatingLeastSquaresComputation].getCanonicalName,
      config, giraphConf, invocation, "als-learning-report_0"))
  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[Als]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: AlsResult): JsObject = returnValue.toJson.asJsObject

  /**
   * The name of the command, e.g. graphs/ml/alternating_least_squares
   */
  override def name: String = "graphs/ml/alternating_least_squares"

  //TODO: Replace with generic code that works on any case class
  override def serializeArguments(arguments: Als): JsObject = arguments.toJson.asJsObject()
}
