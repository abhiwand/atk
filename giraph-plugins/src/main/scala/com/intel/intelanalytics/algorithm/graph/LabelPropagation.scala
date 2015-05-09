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
              vertexValuePropertyList: List[String],
              edgeValuePropertyList: List[String],
              inputEdgeLabelList: List[String],
              outputVertexPropertyList: List[String],
              vectorValue: Boolean,
              maxSupersteps: Option[Int] = None,
              convergenceThreshold: Option[Double] = None,
              anchorThreshold: Option[Double] = None,
              lpLambda: Option[Double] = None,
              validateGraphStructure: Option[Boolean] = None)
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

  override def execute(arguments: Lp)(implicit context: Invocation): LpResult = {

    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")

    val graphFuture = engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "lp.maxSupersteps", arguments.maxSupersteps)
    GiraphConfigurationUtil.set(hConf, "lp.convergenceThreshold", arguments.convergenceThreshold)
    GiraphConfigurationUtil.set(hConf, "lp.anchorThreshold", arguments.anchorThreshold)
    GiraphConfigurationUtil.set(hConf, "lp.bidirectionalCheck", arguments.validateGraphStructure)

    GiraphConfigurationUtil.set(hConf, "giraphjob.maxSteps", arguments.maxSupersteps)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, config, graph)

    GiraphConfigurationUtil.set(hConf, "input.vertex.value.property.key.list", Some(arguments.vertexValuePropertyList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "input.edge.value.property.key.list", Some(arguments.edgeValuePropertyList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", Some(arguments.inputEdgeLabelList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", Some(arguments.outputVertexPropertyList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "vector.value", Some(arguments.vectorValue.toString))

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