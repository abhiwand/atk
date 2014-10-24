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

import com.intel.graphbuilder.elements.{ Vertex => GBVertex, Edge => GBEdge, Property }
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphReference }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.domain.{StorageFormats, DomainJsonProtocol}
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.{ Await, ExecutionContext }
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.graph.GraphName
import spray.json._
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
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
                                   output_graph_name: String)

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

  override def doc = Some(CommandDoc(oneLineSummary = "Connected Components.",
    extendedSummary = Some("""
                             |    Connected components.
                             |
                             |    Parameters
                             |    ----------
                             |    output_property : string
                             |        The name of output property to be added to vertex/edge upon completion
                             |    output_graph_name : string
                             |        The name of output graph to be created (original graph will be left unmodified)
                             |
                             |    Returns
                             |    -------
                             |    graph: String
                             |        Name of the output graph. Call get_graph(graph) to get the handle to the new graph
                             |
                             |    Examples
                             |    --------
                             |        g.ml.graphx_connected_components(output_property = "ccId", output_graph_name = "cc_graph")
                             |
                             |    The expected output is like this::
                             |
                             |        {u'graph': u'cc_graph'}
                             |
                             |    To query::
                             |
                             |        cc_graph = get_graph('cc_graph')
                             |        cc_graph.query.gremlin("g.V [0..4]")
                             |
                             |        {u'results':[{u'_id':4,u'_type':u'vertex',u'b':2335244,u'ccId':278456,
                             |        u'pr':0.967054,u'titanPhysicalId':363016,u'triangle_count':1},{u'_id':8,
                             |        u'_type':u'vertex',u'a':4877684,u'ccId':413036,u'pr':0.967054,
                             |        u'titanPhysicalId':66001228,u'triangle_count':1},{u'_id':12,u'_type':u'vertex',
                             |        u'b':1530344,u'ccId':34280,u'pr':0.967054,u'titanPhysicalId':9912,
                             |        u'triangle_count':1},{u'_id':16,u'_type':u'vertex',u'b':3664209,u'ccId':206980,
                             |        u'pr':0.967054,u'titanPhysicalId':229200900,u'triangle_count':1},{u'_id':20,
                             |        u'_type':u'vertex',u'b':663159,u'ccId':268188,u'pr':0.967054,u'titanPhysicalId':
                             |        268188,u'triangle_count':1}],u'run_time_seconds':0.254}
                             |
                           """.stripMargin)))

  override def execute(sparkInvocation: SparkInvocation, arguments: ConnectedComponentsArgs)(implicit user: UserPrincipal, executionContext: ExecutionContext): ConnectedComponentsResult = {

    sparkInvocation.sparkContext.stop

    val sparkConf: SparkConf = sparkInvocation.sparkContext.getConf.set("spark.kryo.registrator", "com.intel.spark.graphon.GraphonKryoRegistrator")

    val sc = new SparkContext(sparkConf)

    try {

      sc.addJar(Boot.getJar("graphon").getPath)

      // Titan Settings for input
      val config = configuration
      val titanConfig = SparkEngineConfig.titanLoadConfiguration

      // Get the graph
      import scala.concurrent.duration._
      val graph = Await.result(sparkInvocation.engine.getGraph(arguments.graph.id), config.getInt("default-timeout") seconds)

      val iatGraphName = GraphName.convertGraphUserNameToBackendName(graph.name)
      titanConfig.setProperty("storage.tablename", iatGraphName)

      val titanConnector = new TitanGraphConnector(titanConfig)

      // Read the graph from Titan
      val titanReader = new TitanReader(sc, titanConnector)
      val titanReaderRDD = titanReader.read()

      val gbVertices = titanReaderRDD.filterVertices()
      val gbEdges = titanReaderRDD.filterEdges()

      val inputVertices: RDD[Long] = gbVertices.map(gbvertex => gbvertex.physicalId.asInstanceOf[Long])
      val inputEdges = gbEdges
        .map(gbedge => (gbedge.tailPhysicalId.asInstanceOf[Long], gbedge.headPhysicalId.asInstanceOf[Long]))

      // Call ConnectedComponentsGraphXDefault to kick off ConnectedComponents computation on RDDs
      val intermediateVertices = ConnectedComponentsGraphXDefault.run(inputVertices, inputEdges)
      val connectedComponentRDD = intermediateVertices.map({
        case (vertexId, componentId) => (vertexId, Property(arguments.output_property, componentId))
      })

      val outVertices = ConnectedComponentsGraphXDefault.mergeConnectedComponentResult(connectedComponentRDD, gbVertices)

      val newGraphName = arguments.output_graph_name
      val iatNewGraphName = GraphName.convertGraphUserNameToBackendName(newGraphName)
      val newGraph = Await.result(sparkInvocation.engine.createGraph(GraphTemplate(newGraphName,StorageFormats.HBaseTitan)),
        config.getInt("default-timeout") seconds)

      // create titan config copy for newGraph write-back
      val newTitanConfig = new SerializableBaseConfiguration()
      newTitanConfig.copy(titanConfig)
      newTitanConfig.setProperty("storage.tablename", iatNewGraphName)
      writeToTitan(newTitanConfig, outVertices, gbEdges)

      ConnectedComponentsResult(newGraphName)
    }

    finally {
      sc.stop
    }
  }

  // Helper function to write rdds back to Titan
  private def writeToTitan(titanConfig: SerializableBaseConfiguration, gbVertices: RDD[GBVertex], gbEdges: RDD[GBEdge], append: Boolean = false) = {
    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append))
    gb.buildGraphWithSpark(gbVertices, gbEdges)
  }

}

