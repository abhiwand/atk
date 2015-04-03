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

package com.intel.spark.graphon.sampling

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.domain.frame.FrameName
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.graph.GraphBackendName
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.graph.GraphBuilderConfigFactory
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.{ StorageFormats, DomainJsonProtocol }
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphReference }
import spray.json._
import scala.concurrent._
import java.util.UUID
import com.intel.spark.graphon.sampling.VertexSampleSparkOps._
import com.intel.intelanalytics.domain.command.CommandDoc

/**
 * Represents the arguments for vertex sampling
 *
 * @param graph reference to the graph to be sampled
 * @param size the requested sample size
 * @param sampleType type of vertex sampling to use
 * @param seed optional random seed value
 */
case class VertexSampleArguments(graph: GraphReference, size: Int, sampleType: String, seed: Option[Long] = None) {
  require(size >= 1, "Invalid sample size")
  require(sampleType.equals("uniform") ||
    sampleType.equals("degree") ||
    sampleType.equals("degreedist"), "Invalid sample type")
}

/**
 * The result object
 *
 * Note: For now, return the subgraph name, since the current state of things requires the name in order to return a
 * new Frame instance in Python.
 *
 * @param name name of the subgraph
 */
case class VertexSampleResult(name: String)

/** Json conversion for arguments and return value case classes */
object VertexSampleJsonFormat {
  import DomainJsonProtocol._
  implicit val vertexSampleFormat = jsonFormat4(VertexSampleArguments)
  implicit val vertexSampleResultFormat = jsonFormat1(VertexSampleResult)
}

import VertexSampleJsonFormat._

class VertexSample extends SparkCommandPlugin[VertexSampleArguments, VertexSampleResult] {

  /**
   * The name of the command
   */
  override def name: String = "graph:titan/sampling/vertex_sample"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: VertexSampleArguments)(implicit invocation: Invocation): VertexSampleResult = {

    // get the input graph object
    val graph = engine.graphs.expectGraph(arguments.graph)

    // get SparkContext and add the graphon jar
    if (sc.master != "yarn-cluster")
      sc.addJar(SparkContextFactory.jarPath("graphon"))

    // convert graph name and get the graph vertex and edge RDDs
    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)

    val vertexSample = arguments.sampleType match {
      case "uniform" => sampleVerticesUniform(gbVertices, arguments.size, arguments.seed)
      case "degree" => sampleVerticesDegree(gbVertices, gbEdges, arguments.size, arguments.seed)
      case "degreedist" => sampleVerticesDegreeDist(gbVertices, gbEdges, arguments.size, arguments.seed)
      case _ => throw new IllegalArgumentException("Invalid sample type")
    }

    // get the vertex induced subgraph edges
    val edgeSample = vertexInducedEdgeSet(vertexSample, gbEdges)

    // strip '-' character so UUID format is consistent with the Python generated UUID format
    val subgraphName = Some(FrameName.generate(prefix = Some("graph_")))

    val subgraph = engine.graphs.createGraph(GraphTemplate(subgraphName, StorageFormats.HBaseTitan))

    // create titan config copy for subgraph write-back
    val subgraphTitanConfig = GraphBuilderConfigFactory.getTitanConfiguration(subgraph.name.get)

    writeToTitan(vertexSample, edgeSample, subgraphTitanConfig)

    VertexSampleResult(subgraphName.get)
  }

}
