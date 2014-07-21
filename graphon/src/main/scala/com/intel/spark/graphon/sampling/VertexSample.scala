////////////////////////////////////////////////////////////////////////////////
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
////////////////////////////////////////////////////////////////////////////////

package com.intel.spark.graphon.sampling

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.spark.graph.GraphName
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphReference }
import spray.json._
import scala.concurrent._
import java.util.UUID
import com.intel.spark.graphon.sampling.SamplingSparkOps._

/**
 * @param graph reference to the graph to be sampled
 * @param size the requested sample size
 * @param sampleType type of vertex sampling to use
 * @param seed random seed value
 */
case class VS(graph: GraphReference, size: Int, sampleType: String, seed: Option[Long] = None) {
  require(size >= 1, "Invalid sample size")
  require(sampleType.equals("uniform") ||
    sampleType.equals("degree") ||
    sampleType.equals("degreedist"), "Invalid sample type")
}

case class VSResult(subgraph: GraphReference)

class VertexSample extends SparkCommandPlugin[VS, VSResult] {

  import DomainJsonProtocol._

  implicit val vsFormat = jsonFormat4(VS)
  implicit val vsResultFormat = jsonFormat1(VSResult)

  override def execute(invocation: SparkInvocation, arguments: VS)(implicit user: UserPrincipal, executionContext: ExecutionContext): VSResult = {

    // Titan Settings
    val config = configuration
    val titanConfigInput = config.getConfig("titan.load")

    // create titanConfig and a copy for the subgraph write-back
    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", titanConfigInput.getString("storage.backend"))
    titanConfig.setProperty("storage.hostname", titanConfigInput.getString("storage.hostname"))
    titanConfig.setProperty("storage.port", titanConfigInput.getString("storage.port"))

    val subgraphTitanConfig = new SerializableBaseConfiguration()
    subgraphTitanConfig.copy(titanConfig)

    import scala.concurrent.duration._
    val graph = Await.result(invocation.engine.getGraph(arguments.graph.id), config.getInt("default-timeout") seconds)

    val sc = invocation.sparkContext
    sc.addJar(Boot.getJar("graphon").getPath)

    val iatGraphName = GraphName.convertGraphUserNameToBackendName(graph.name)
    val (vertexRDD, edgeRDD) = getGraph(iatGraphName, sc, titanConfig)

    val vertexSample = arguments.sampleType match {
      case "uniform" => sampleVerticesUniform(vertexRDD, arguments.size, arguments.seed)
      case "degree" => sampleVerticesDegree(vertexRDD, edgeRDD, arguments.size, arguments.seed)
      case "degreedist" => sampleVerticesDegreeDist(vertexRDD, edgeRDD, arguments.size, arguments.seed)
      case _ => throw new IllegalArgumentException("Invalid sample type")
    }

    val edgeSample = sampleEdges(vertexSample, edgeRDD)

    val iatSubgraphName = GraphName.convertGraphUserNameToBackendName("graph_" + UUID.randomUUID.toString)
    subgraphTitanConfig.setProperty("storage.tablename", iatSubgraphName)

    val subgraph = Await.result(invocation.engine.createGraph(GraphTemplate(iatSubgraphName)), config.getInt("default-timeout") seconds)

    writeToTitan(vertexSample, edgeSample, subgraphTitanConfig)

    VSResult(new GraphReference(subgraph.id))
  }

  /**
   * The name of the command
   */
  override def name: String = "graphs/sampling/vertex_sample"

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[VS]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: VSResult): JsObject = returnValue.toJson.asJsObject

  //TODO: Replace with generic code that works on any case class
  override def serializeArguments(arguments: VS): JsObject = arguments.toJson.asJsObject()

}
