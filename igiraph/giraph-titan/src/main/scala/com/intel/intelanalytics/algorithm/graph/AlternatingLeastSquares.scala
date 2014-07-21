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
import com.intel.intelanalytics.algorithm.util.{ GiraphConfigurationUtil, GiraphJobDriver }
import org.apache.giraph.conf.GiraphConfiguration
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent.duration._

import scala.concurrent._
import scala.collection.JavaConverters._

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

  override def execute(invocation: Invocation, arguments: Als)(implicit user: UserPrincipal, executionContext: ExecutionContext): AlsResult = {

    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")
    val titanConf = GiraphConfigurationUtil.flattenConfig(config.getConfig("titan"), "titan.")

    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "als.maxSuperSteps", arguments.max_supersteps)
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

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanHBaseVertexInputFormatPropertyGraph4CF])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4CF[_ <: org.apache.hadoop.io.WritableComparable[_], _ <: org.apache.hadoop.io.Writable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[AlternatingLeastSquaresComputation.AlternatingLeastSquaresMasterCompute])
    giraphConf.setComputationClass(classOf[AlternatingLeastSquaresComputation])
    giraphConf.setAggregatorWriterClass(classOf[AlternatingLeastSquaresComputation.AlternatingLeastSquaresAggregatorWriter])

    AlsResult(GiraphJobDriver.run("ia_giraph_als",
      classOf[AlternatingLeastSquaresComputation].getCanonicalName,
      config, giraphConf, invocation.commandId, "als-learning-report_0"))
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
