/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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

    if (!SparkEngineConfig.isSparkOnYarn) {
      sc.addJar(SparkContextFactory.jarPath("graph-plugins"))
    }

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
