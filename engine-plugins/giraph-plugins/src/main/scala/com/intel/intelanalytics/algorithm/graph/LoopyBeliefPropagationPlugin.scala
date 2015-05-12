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

import com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation
import com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation.{ LoopyBeliefPropagationAggregatorWriter, LoopyBeliefPropagationMasterCompute }
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

case class LoopyBeliefPropagation(graph: GraphReference,
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

case class LoopyBeliefPropagationResult(value: String) //TODO

/** Json conversion for arguments and return value case classes */
object LoopyBeliefPropagationJsonFormat {
  import DomainJsonProtocol._
  implicit val lbpFormat = jsonFormat15(LoopyBeliefPropagation)
  implicit val lbpResultFormat = jsonFormat1(LoopyBeliefPropagationResult)
}

import LoopyBeliefPropagationJsonFormat._

class LoopyBeliefPropagationPlugin
    extends CommandPlugin[LoopyBeliefPropagation, LoopyBeliefPropagationResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph/ml/loopy_belief_propagation"

  override def execute(arguments: LoopyBeliefPropagation)(implicit invocation: Invocation): LoopyBeliefPropagationResult = {

    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")

    val graphFuture = engine.getGraph(arguments.graph.id)
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

    GiraphConfigurationUtil.set(hConf, "giraphjob.maxSteps", arguments.maxSupersteps)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, config, graph)

    GiraphConfigurationUtil.set(hConf, "input.vertex.value.property.key.list", Some(arguments.vertexValuePropertyList.mkString(argSeparator)))
    GiraphConfigurationUtil.set(hConf, "input.edge.value.property.key.list", Some(arguments.edgeValuePropertyList.mkString(argSeparator)))
    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", Some(arguments.inputEdgeLabelList.mkString(argSeparator)))
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", Some(arguments.outputVertexPropertyList.mkString(argSeparator)))
    GiraphConfigurationUtil.set(hConf, "vertex.type.property.key", Some(arguments.vertexType))
    GiraphConfigurationUtil.set(hConf, "vector.value", Some(arguments.vectorValue.toString))

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanVertexInputFormatPropertyGraph4LBP])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4LBP[_ <: org.apache.hadoop.io.WritableComparable[_], _ <: org.apache.hadoop.io.Writable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[LoopyBeliefPropagationMasterCompute])
    giraphConf.setComputationClass(classOf[LoopyBeliefPropagationComputation])
    giraphConf.setAggregatorWriterClass(classOf[LoopyBeliefPropagationAggregatorWriter])

    LoopyBeliefPropagationResult(GiraphJobManager.run("ia_giraph_lbp",
      classOf[LoopyBeliefPropagationComputation].getCanonicalName,
      config, giraphConf, invocation, "lbp-learning-report_0"))
  }

}
