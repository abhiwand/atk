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
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4LDA
import com.intel.giraph.io.titan.TitanVertexOutputFormatPropertyGraph4LDA
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.algorithm.util.{ GiraphConfigurationUtil, GiraphJobDriver }
import com.intel.mahout.math.DoubleWithVectorWritable
import org.apache.giraph.conf.GiraphConfiguration
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent.duration._

import scala.concurrent._
import scala.collection.JavaConverters._

case class Lda(graph: GraphReference,
               edge_value_property_list: Option[String],
               input_edge_label_list: Option[String],
               output_vertex_property_list: Option[String],
               vertex_type_property_key: Option[String],
               vector_value: Option[String],
               max_supersteps: Option[Int] = None,
               alpha: Option[Float] = None,
               beta: Option[Float] = None,
               convergence_threshold: Option[Double] = None,
               evaluation_cost: Option[Boolean] = None,
               max_value: Option[Float] = None,
               min_value: Option[Float] = None,
               bidirectional_check: Option[Boolean] = None,
               num_topics: Option[Int] = None)

case class LdaResult(value: String)

class LatentDirichletAllocation
    extends CommandPlugin[Lda, LdaResult] {
  import DomainJsonProtocol._
  implicit val ldaFormat = jsonFormat15(Lda)
  implicit val ldaResultFormat = jsonFormat1(LdaResult)

  override def execute(invocation: Invocation, arguments: Lda)(implicit user: UserPrincipal, executionContext: ExecutionContext): LdaResult = {

    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")
    val titanConf = GiraphConfigurationUtil.flattenConfig(config.getConfig("titan"), "titan.")

    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "lda.maxSuperSteps", arguments.max_supersteps)
    GiraphConfigurationUtil.set(hConf, "lda.alpha", arguments.alpha)
    GiraphConfigurationUtil.set(hConf, "lda.beta", arguments.beta)
    GiraphConfigurationUtil.set(hConf, "lda.convergenceThreshold", arguments.convergence_threshold)
    GiraphConfigurationUtil.set(hConf, "lda.evaluateCost", arguments.evaluation_cost)
    GiraphConfigurationUtil.set(hConf, "lda.maxVal", arguments.max_value)
    GiraphConfigurationUtil.set(hConf, "lda.minVal", arguments.min_value)
    GiraphConfigurationUtil.set(hConf, "lda.bidirectionalCheck", arguments.bidirectional_check)
    GiraphConfigurationUtil.set(hConf, "lda.numTopics", arguments.num_topics)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, titanConf, graph)

    GiraphConfigurationUtil.set(hConf, "input.edge.value.property.key.list", arguments.edge_value_property_list)
    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", arguments.input_edge_label_list)
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", arguments.output_vertex_property_list)
    GiraphConfigurationUtil.set(hConf, "vertex.type.property.key", arguments.vertex_type_property_key)
    GiraphConfigurationUtil.set(hConf, "vector.value", arguments.vector_value)

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanHBaseVertexInputFormatPropertyGraph4LDA])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4LDA[_ <: org.apache.hadoop.io.LongWritable, _ <: com.intel.giraph.io.VertexData4LPWritable, _ <: com.intel.mahout.math.DoubleWithVectorWritable]])
    giraphConf.setMasterComputeClass(classOf[CVB0LDAComputation.CVB0LDAMasterCompute])
    giraphConf.setComputationClass(classOf[CVB0LDAComputation])
    giraphConf.setAggregatorWriterClass(classOf[CVB0LDAComputation.CVB0LDAAggregatorWriter])

    LdaResult(GiraphJobDriver.run("ia_giraph_lda",
      classOf[CVB0LDAComputation].getCanonicalName,
      config, giraphConf, invocation.commandId))
  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[Lda]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: LdaResult): JsObject = returnValue.toJson.asJsObject

  override def name: String = "graphs/ml/latent_dirichlet_allocation"

  //TODO: Replace with generic code that works on any case class
  override def serializeArguments(arguments: Lda): JsObject = arguments.toJson.asJsObject()
}
