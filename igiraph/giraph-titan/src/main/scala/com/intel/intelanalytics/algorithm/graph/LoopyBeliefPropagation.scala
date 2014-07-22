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
import com.intel.giraph.io.titan.TitanVertexOutputFormatPropertyGraph4LBP
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4LBP
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

case class Lbp(graph: GraphReference,
               vertex_value_property_list: Option[String],
               edge_value_property_list: Option[String],
               input_edge_label_list: Option[String],
               output_vertex_property_list: Option[String],
               vertex_type_property_key: Option[String],
               vector_value: Option[String],
               max_supersteps: Option[Int] = None,
               convergence_threshold: Option[Double] = None,
               anchor_threshold: Option[Double] = None,
               smoothing: Option[Double] = None,
               bidirectional_check: Option[Boolean] = None,
               ignore_vertex_type: Option[Boolean] = None,
               max_product: Option[Boolean] = None,
               power: Option[Double] = None)

case class LbpResult(value: String) //TODO

class LoopyBeliefPropagation
    extends CommandPlugin[Lbp, LbpResult] {
  import DomainJsonProtocol._
  implicit val lbpFormat = jsonFormat15(Lbp)
  implicit val lbpResultFormat = jsonFormat1(LbpResult)

  override def execute(invocation: Invocation, arguments: Lbp)(implicit user: UserPrincipal, executionContext: ExecutionContext): LbpResult = {

    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")
    val titanConf = GiraphConfigurationUtil.flattenConfig(config.getConfig("titan"), "titan.")

    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "lbp.maxSuperSteps", arguments.max_supersteps)
    GiraphConfigurationUtil.set(hConf, "lbp.convergenceThreshold", arguments.convergence_threshold)
    GiraphConfigurationUtil.set(hConf, "lbp.anchorThreshold", arguments.anchor_threshold)
    GiraphConfigurationUtil.set(hConf, "lbp.bidirectionalCheck", arguments.bidirectional_check)
    GiraphConfigurationUtil.set(hConf, "lbp.power", arguments.power)
    GiraphConfigurationUtil.set(hConf, "lbp.smoothing", arguments.smoothing)
    GiraphConfigurationUtil.set(hConf, "lbp.ignoreVertexType", arguments.ignore_vertex_type)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, titanConf, graph)

    GiraphConfigurationUtil.set(hConf, "input.vertex.value.property.key.list", arguments.vertex_value_property_list)
    GiraphConfigurationUtil.set(hConf, "input.edge.value.property.key.list", arguments.edge_value_property_list)
    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", arguments.input_edge_label_list)
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", arguments.output_vertex_property_list)
    GiraphConfigurationUtil.set(hConf, "vertex.type.property.key", arguments.vertex_type_property_key)
    GiraphConfigurationUtil.set(hConf, "vector.value", arguments.vector_value)

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanHBaseVertexInputFormatPropertyGraph4LBP])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4LBP[_ <: org.apache.hadoop.io.WritableComparable[_], _ <: org.apache.hadoop.io.Writable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[LoopyBeliefPropagationComputation.LoopyBeliefPropagationMasterCompute])
    giraphConf.setComputationClass(classOf[LoopyBeliefPropagationComputation])
    giraphConf.setAggregatorWriterClass(classOf[LoopyBeliefPropagationComputation.LoopyBeliefPropagationAggregatorWriter])

    LbpResult(GiraphJobDriver.run("ia_giraph_lbp",
      classOf[LoopyBeliefPropagationComputation].getCanonicalName,
      config, giraphConf, invocation.commandId, "lbp-learning-report_0"))
  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[Lbp]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: LbpResult): JsObject = returnValue.toJson.asJsObject

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   */
  override def name: String = "graphs/ml/loopy_belief_propagation"

  //TODO: Replace with generic code that works on any case class
  override def serializeArguments(arguments: Lbp): JsObject = arguments.toJson.asJsObject()
}
