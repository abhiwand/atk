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
import com.intel.giraph.io.titan.hbase.{ TitanHBaseVertexInputFormatPropertyGraph4CFCGD, TitanHBaseVertexInputFormatPropertyGraph4CF }
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

case class Cgd(graph: GraphReference,
               edge_value_property_list: Option[String],
               input_edge_label_list: Option[String],
               output_vertex_property_list: Option[String],
               vertex_type_property_key: Option[String],
               edge_type_property_key: Option[String],
               vector_value: Option[String],
               max_supersteps: Option[Int] = None,
               convergence_threshold: Option[Double] = None,
               cgd_lambda: Option[Float] = None,
               feature_dimension: Option[Int] = None,
               learning_curve_output_interval: Option[Int] = None,
               bidirectional_check: Option[Boolean] = None,
               bias_on: Option[Boolean] = None,
               max_value: Option[Float] = None,
               min_value: Option[Float] = None,
               num_iters: Option[Int] = None)

case class CgdResult(value: String)

class ConjugateGradientDescent
    extends CommandPlugin[Cgd, CgdResult] {
  import DomainJsonProtocol._
  implicit val cgdFormat = jsonFormat17(Cgd)
  implicit val cgdResultFormat = jsonFormat1(CgdResult)

  override def execute(invocation: Invocation, arguments: Cgd)(implicit user: UserPrincipal, executionContext: ExecutionContext): CgdResult = {

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
    GiraphConfigurationUtil.set(hConf, "cgd.maxSuperSteps", arguments.max_supersteps)
    GiraphConfigurationUtil.set(hConf, "cgd.convergenceThreshold", arguments.convergence_threshold)
    GiraphConfigurationUtil.set(hConf, "cgd.featureDimension", arguments.feature_dimension)
    GiraphConfigurationUtil.set(hConf, "cgd.bidirectionalCheck", arguments.bidirectional_check)
    GiraphConfigurationUtil.set(hConf, "cgd.biasOn", arguments.bias_on)
    GiraphConfigurationUtil.set(hConf, "cgd.lambda", arguments.cgd_lambda)
    GiraphConfigurationUtil.set(hConf, "cgd.learningCurveOutputInterval", arguments.learning_curve_output_interval)
    GiraphConfigurationUtil.set(hConf, "cgd.maxVal", arguments.max_value)
    GiraphConfigurationUtil.set(hConf, "cgd.minVal", arguments.min_value)
    GiraphConfigurationUtil.set(hConf, "cgd.numCGDIters", arguments.num_iters)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, titanConf, graph)

    GiraphConfigurationUtil.set(hConf, "input.edge.value.property.key.list", arguments.edge_value_property_list)
    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", arguments.input_edge_label_list)
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", arguments.output_vertex_property_list)
    GiraphConfigurationUtil.set(hConf, "vertex.type.property.key", arguments.vertex_type_property_key)
    GiraphConfigurationUtil.set(hConf, "edge.type.property.key", arguments.edge_type_property_key)
    GiraphConfigurationUtil.set(hConf, "vector.value", arguments.vector_value)
    GiraphConfigurationUtil.set(hConf, "output.vertex.bias", biasOnOption)

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanHBaseVertexInputFormatPropertyGraph4CFCGD])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4CF[_ <: org.apache.hadoop.io.WritableComparable[_], _ <: org.apache.hadoop.io.Writable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[ConjugateGradientDescentComputation.ConjugateGradientDescentMasterCompute])
    giraphConf.setComputationClass(classOf[ConjugateGradientDescentComputation])
    giraphConf.setAggregatorWriterClass(classOf[ConjugateGradientDescentComputation.ConjugateGradientDescentAggregatorWriter])

    CgdResult(GiraphJobManager.run("ia_giraph_cgd",
      classOf[ConjugateGradientDescentComputation].getCanonicalName,
      config, giraphConf, invocation, "cgd-learning-report_0"))
  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[Cgd]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: CgdResult): JsObject = returnValue.toJson.asJsObject

  /**
   * The name of the command, e.g. graphs/ml/alternating_least_squares
   */
  override def name: String = "graphs/ml/conjugate_gradient_descent"

  //TODO: Replace with generic code that works on any case class
  override def serializeArguments(arguments: Cgd): JsObject = arguments.toJson.asJsObject()
}
