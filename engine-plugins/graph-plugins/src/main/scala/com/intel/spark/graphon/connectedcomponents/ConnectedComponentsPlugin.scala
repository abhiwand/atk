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

package com.intel.spark.graphon.connectedcomponents

import com.intel.graphbuilder.elements.{ GBVertex, GBEdge, Property }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.frame.{ FrameMeta, FrameEntity }
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphReference }
import com.intel.intelanalytics.domain.schema.VertexSchema
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData, RowWrapper, SparkFrameStorage }
import com.intel.intelanalytics.engine.spark.graph.plugins.exportfromtitan.{ ExportToGraphPlugin, VertexSchemaAggregator }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.domain.{ CreateEntityArgs, StorageFormats, DomainJsonProtocol }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.spark.graphon.clusteringcoefficient.FrameSchemaAggregator
import org.apache.spark.frame.FrameRdd
import org.apache.spark.ia.graph.VertexWrapper
import org.apache.spark.storage.StorageLevel
import scala.concurrent.{ Await, ExecutionContext }
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.graph.{ SparkGraphStorage, GraphBuilderConfigFactory }
import spray.json._
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import com.intel.intelanalytics.domain.command.CommandDoc
import org.apache.spark.{ SparkConf, SparkContext }
import DomainJsonProtocol._
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

import java.util.UUID

/**
 * Parameters for executing connected components.
 * @param graph Reference to the graph object on which to compute connected components.
 * @param outputProperty Name of the property to which connected components value will be stored on vertex and edge.
 */
case class ConnectedComponentsArgs(graph: GraphReference,
                                   @ArgDoc("""Name of the property to which connected components value will be stored on vertex and edge.""") outputProperty: String) {
  require(!outputProperty.isEmpty, "Output property label must be provided")
}

case class ConnectedComponentsReturn(frameDictionaryOutput: Map[String, FrameEntity])

/** Json conversion for arguments and return value case classes */
object ConnectedComponentsJsonFormat {
  import DomainJsonProtocol._
  implicit val CCArgsFormat = jsonFormat2(ConnectedComponentsArgs)
  implicit val CCReturnFormat = jsonFormat1(ConnectedComponentsReturn)
}

import ConnectedComponentsJsonFormat._

@PluginDoc(oneLine = "Implements the connected components computation on a graph by invoking graphx api.",
  extended = """Pulls graph from underlying store, sends it off to the ConnectedComponentGraphXDefault,
and then writes the output graph back to the underlying store.

Right now it is using only Titan for graph storage. Other backends including Parquet will be supported later.""")
class ConnectedComponentsPlugin extends SparkCommandPlugin[ConnectedComponentsArgs, ConnectedComponentsReturn] {
  override def name: String = "graph/graphx_connected_components"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: ConnectedComponentsArgs)(implicit invocation: Invocation): ConnectedComponentsReturn = {

    // Get the graph
    val graph = engine.graphs.expectGraph(arguments.graph)

    // Read the graph from Titan
    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)

    val inputVertices: RDD[Long] = gbVertices.map(gbvertex => gbvertex.physicalId.asInstanceOf[Long])
    val inputEdges = gbEdges.map(gbedge => (gbedge.tailPhysicalId.asInstanceOf[Long], gbedge.headPhysicalId.asInstanceOf[Long]))

    // Call ConnectedComponentsGraphXDefault to kick off ConnectedComponents computation on RDDs
    val intermediateVertices = ConnectedComponentsGraphXDefault.run(inputVertices, inputEdges)
    val connectedComponentRDD = intermediateVertices.map({
      case (vertexId, componentId) => (vertexId, Property(arguments.outputProperty, componentId))
    })

    val outVertices = ConnectedComponentsGraphXDefault.mergeConnectedComponentResult(connectedComponentRDD, gbVertices)

    val frameRddMap = FrameRdd.toFrameRddMap(outVertices)

    new ConnectedComponentsReturn(frameRddMap.keys.map(label => {
      val result = tryNew(CreateEntityArgs(description = Some("created by connected components operation"))) { newOutputFrame: FrameMeta =>
        val frameRdd = frameRddMap(label)
        save(new SparkFrameData(newOutputFrame.meta, frameRdd))
      }.meta
      (label, result)
    }).toMap)

  }

}
