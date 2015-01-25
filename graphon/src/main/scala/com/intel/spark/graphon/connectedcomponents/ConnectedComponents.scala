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

package com.intel.spark.graphon.connectedcomponents

import com.intel.graphbuilder.elements.{ GBVertex, GBEdge, Property }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphReference }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.domain.{ StorageFormats, DomainJsonProtocol }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.storage.StorageLevel
import scala.concurrent.{ Await, ExecutionContext }
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.graph.GraphBuilderConfigFactory
import spray.json._
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.intelanalytics.domain.command.CommandDoc
import org.apache.spark.{ SparkConf, SparkContext }
import java.util.UUID

/**
 * Parameters for executing connected components.
 * @param graph Reference to the graph object on which to compute connected components.
 * @param output_property Name of the property to which connected components value will be stored on vertex and edge.
 * @param output_graph_name Name of output graph.
 */
case class ConnectedComponentsArgs(graph: GraphReference,
                                   output_property: String,
                                   output_graph_name: String) {
  require(!output_property.isEmpty, "Output property label must be provided")
  require(!output_graph_name.isEmpty, "Output graph name must be provided")
}

/**
 * The result object
 * @param graph Name of the output graph
 */
case class ConnectedComponentsResult(graph: String)

/** Json conversion for arguments and return value case classes */
object ConnectedComponentsJsonFormat {
  import DomainJsonProtocol._
  implicit val CCFormat = jsonFormat3(ConnectedComponentsArgs)
  implicit val CCResultFormat = jsonFormat1(ConnectedComponentsResult)
}

import ConnectedComponentsJsonFormat._

/**
 * ConnectedComponent plugin implements the connected components computation on a graph by invoking graphx api.
 *
 * Pulls graph from underlying store, sends it off to the ConnectedComponentGraphXDefault, and then writes the output graph
 * back to the underlying store.
 *
 * Right now it is using only Titan for graph storage. Other backends including Parquet will be supported later.
 */
class ConnectedComponents extends SparkCommandPlugin[ConnectedComponentsArgs, ConnectedComponentsResult] {

  override def name: String = "graph:titan/ml/graphx_connected_components"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: ConnectedComponentsArgs)(implicit invocation: Invocation): ConnectedComponentsResult = {

    val sparkContext = sc

    sparkContext.addJar(SparkContextFactory.jarPath("graphon"))

    // Titan Settings for input
    val config = configuration

    // Get the graph
    import scala.concurrent.duration._
    val graph = Await.result(engine.getGraph(arguments.graph.id), config.getInt("default-timeout") seconds)

    // Read the graph from Titan
    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)

    gbVertices.persist(StorageLevel.MEMORY_AND_DISK_SER)
    gbEdges.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val inputVertices: RDD[Long] = gbVertices
      .map(gbvertex => gbvertex.physicalId.asInstanceOf[Long])

    val inputEdges = gbEdges
      .map(gbedge => (gbedge.tailPhysicalId.asInstanceOf[Long], gbedge.headPhysicalId.asInstanceOf[Long]))

    // Call ConnectedComponentsGraphXDefault to kick off ConnectedComponents computation on RDDs
    val intermediateVertices = ConnectedComponentsGraphXDefault.run(inputVertices, inputEdges)
    val connectedComponentRDD = intermediateVertices.map({
      case (vertexId, componentId) => (vertexId, Property(arguments.output_property, componentId))
    })

    val outVertices = ConnectedComponentsGraphXDefault.mergeConnectedComponentResult(connectedComponentRDD, gbVertices)

    val newGraphName = Some(arguments.output_graph_name)
    val newGraph = Await.result(engine.createGraph(GraphTemplate(newGraphName, StorageFormats.HBaseTitan)),
      config.getInt("default-timeout") seconds)

    // create titan config copy for newGraph write-back
    val newTitanConfig = GraphBuilderConfigFactory.getTitanConfiguration(newGraph.name.get)
    writeToTitan(newTitanConfig, outVertices, gbEdges)

    gbVertices.unpersist()
    gbEdges.unpersist()

    ConnectedComponentsResult(newGraphName.get)

  }

  // Helper function to write rdds back to Titan
  private def writeToTitan(titanConfig: SerializableBaseConfiguration, gbVertices: RDD[GBVertex], gbEdges: RDD[GBEdge], append: Boolean = false) = {
    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append))
    gb.buildGraphWithSpark(gbVertices, gbEdges)
  }

}

