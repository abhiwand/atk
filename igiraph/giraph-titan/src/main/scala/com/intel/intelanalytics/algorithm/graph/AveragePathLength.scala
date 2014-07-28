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

import com.intel.giraph.algorithms.apl.AveragePathLengthComputation
import com.intel.giraph.io.titan.TitanVertexOutputFormatLongIDDistanceMap
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatLongDistanceMapNull
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.algorithm.util.{ GiraphConfigurationUtil, GiraphJobManager }
import org.apache.giraph.conf.GiraphConfiguration
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent.duration._

import scala.concurrent._

case class Apl(graph: GraphReference,
               input_edge_label_list: Option[String],
               output_vertex_property_list: Option[List[String]],
               convergence_progress_output_interval: Option[Int] = None) {
  require(output_vertex_property_list.get != null && output_vertex_property_list.get.size == 2,
    "output_vertex_property_list should enlist 2 properties (comma-separated) to store path sum and path count")
}

case class AplResult(value: String) //TODO

class AveragePathLength
    extends CommandPlugin[Apl, AplResult] {
  import DomainJsonProtocol._
  implicit val aplFormat = jsonFormat4(Apl)
  implicit val aplResultFormat = jsonFormat1(AplResult)

  override def execute(invocation: Invocation, arguments: Apl)(implicit user: UserPrincipal, executionContext: ExecutionContext): AplResult = {

    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")
    val titanConf = GiraphConfigurationUtil.flattenConfig(config.getConfig("titan"), "titan.")

    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "apl.convergenceProgressOutputInterval", arguments.convergence_progress_output_interval)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, titanConf, graph)

    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", arguments.input_edge_label_list)
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", Option[Any](arguments.output_vertex_property_list.get.mkString(",")))

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanHBaseVertexInputFormatLongDistanceMapNull])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatLongIDDistanceMap[_ <: org.apache.hadoop.io.LongWritable, _ <: com.intel.giraph.io.DistanceMapWritable, _ <: org.apache.hadoop.io.NullWritable]])
    giraphConf.setMasterComputeClass(classOf[AveragePathLengthComputation.AveragePathLengthMasterCompute])
    giraphConf.setComputationClass(classOf[AveragePathLengthComputation])
    giraphConf.setAggregatorWriterClass(classOf[AveragePathLengthComputation.AveragePathLengthAggregatorWriter])

    AplResult(GiraphJobManager.run("ia_giraph_apl",
      classOf[AveragePathLengthComputation].getCanonicalName,
      config, giraphConf, invocation, "apl-convergence-report_0"))
  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[Apl]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: AplResult): JsObject = returnValue.toJson.asJsObject

  override def name: String = "graphs/ml/average_path_length"

  //TODO: Replace with generic code that works on any case class
  override def serializeArguments(arguments: Apl): JsObject = arguments.toJson.asJsObject()
}
