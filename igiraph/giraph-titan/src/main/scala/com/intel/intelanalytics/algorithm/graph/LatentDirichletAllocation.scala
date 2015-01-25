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

import com.intel.giraph.algorithms.lda.CVB0LDAComputation
import com.intel.giraph.io.VertexData4LDAWritable
import com.intel.giraph.io.titan.formats.{ TitanVertexOutputFormatPropertyGraph4LDA, TitanVertexInputFormatPropertyGraph4LDA }
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.algorithm.util.{ GiraphJobManager, GiraphConfigurationUtil }
import com.intel.mahout.math.DoubleWithVectorWritable
import org.apache.giraph.conf.GiraphConfiguration
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent.duration._

import scala.concurrent._
import scala.collection.JavaConverters._
import com.intel.intelanalytics.domain.command.CommandDoc

case class Lda(graph: GraphReference,
               edgeValuePropertyList: List[String],
               inputEdgeLabelList: List[String],
               outputVertexPropertyList: List[String],
               vertexType: String,
               vectorValue: Boolean,
               maxSupersteps: Option[Int] = None,
               alpha: Option[Float] = None,
               beta: Option[Float] = None,
               convergenceThreshold: Option[Double] = None,
               evaluationCost: Option[Boolean] = None,
               maxValue: Option[Float] = None,
               minValue: Option[Float] = None,
               validateGraphStructure: Option[Boolean] = None,
               numTopics: Option[Int] = None)

case class LdaResult(value: String)

/** Json conversion for arguments and return value case classes */
object LdaJsonFormat {
  import DomainJsonProtocol._
  implicit val ldaFormat = jsonFormat15(Lda)
  implicit val ldaResultFormat = jsonFormat1(LdaResult)
}

import LdaJsonFormat._

class LatentDirichletAllocation
    extends CommandPlugin[Lda, LdaResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:titan/ml/latent_dirichlet_allocation"

  override def execute(arguments: Lda)(implicit invocation: Invocation): LdaResult = {

    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")

    val graphFuture = engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "lda.maxSupersteps", arguments.maxSupersteps)
    GiraphConfigurationUtil.set(hConf, "lda.alpha", arguments.alpha)
    GiraphConfigurationUtil.set(hConf, "lda.beta", arguments.beta)
    GiraphConfigurationUtil.set(hConf, "lda.convergenceThreshold", arguments.convergenceThreshold)
    GiraphConfigurationUtil.set(hConf, "lda.evaluateCost", arguments.evaluationCost)
    GiraphConfigurationUtil.set(hConf, "lda.maxVal", arguments.maxValue)
    GiraphConfigurationUtil.set(hConf, "lda.minVal", arguments.minValue)
    GiraphConfigurationUtil.set(hConf, "lda.bidirectionalCheck", arguments.validateGraphStructure)
    GiraphConfigurationUtil.set(hConf, "lda.numTopics", arguments.numTopics)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, config, graph)

    GiraphConfigurationUtil.set(hConf, "input.edge.value.property.key.list", Some(arguments.edgeValuePropertyList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", Some(arguments.inputEdgeLabelList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", Some(arguments.outputVertexPropertyList.mkString(",")))
    GiraphConfigurationUtil.set(hConf, "vertex.type.property.key", Some(arguments.vertexType))
    GiraphConfigurationUtil.set(hConf, "vector.value", Some(arguments.vectorValue.toString))

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanVertexInputFormatPropertyGraph4LDA])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4LDA[_ <: org.apache.hadoop.io.LongWritable, _ <: com.intel.giraph.io.VertexData4LPWritable, _ <: com.intel.mahout.math.DoubleWithVectorWritable]])
    giraphConf.setMasterComputeClass(classOf[CVB0LDAComputation.CVB0LDAMasterCompute])
    giraphConf.setComputationClass(classOf[CVB0LDAComputation])
    giraphConf.setAggregatorWriterClass(classOf[CVB0LDAComputation.CVB0LDAAggregatorWriter])

    LdaResult(GiraphJobManager.run("ia_giraph_lda",
      classOf[CVB0LDAComputation].getCanonicalName,
      config, giraphConf, invocation, "lda-learning-report_0"))
  }

}
