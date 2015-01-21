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

    val graphFuture = engine.getGraph(arguments.graph.id)
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
