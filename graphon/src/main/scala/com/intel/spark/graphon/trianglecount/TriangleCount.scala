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

package com.intel.spark.graphon.trianglecount

import com.intel.graphbuilder.driver.spark.titan.GraphBuilder
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.{ StorageFormats, DomainJsonProtocol }
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphReference }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.storage.StorageLevel
import scala.concurrent.{ Await, ExecutionContext }
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.graph.GraphBuilderConfigFactory
import spray.json._
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ GBVertex, GBEdge }
import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.intelanalytics.domain.command.CommandDoc
import org.apache.spark.{ SparkConf, SparkContext }
import java.util.UUID

/**
 * Parameters for executing triangle count.
 * @param graph Reference to the graph object on which to compute triangle count.
 * @param output_property Name of the property to which triangle count value will be stored on vertex.
 * @param output_graph_name Name of output graph.
 * @param input_edge_labels List of edge labels to consider for computation. If None, all edges are considered.
 */
case class TriangleCountArgs(graph: GraphReference,
                             output_property: String,
                             output_graph_name: String,
                             input_edge_labels: Option[List[String]] = None) {
  require(!output_property.isEmpty, "Output property label must be provided")
  require(!output_graph_name.isEmpty, "Output graph name must be provided")
}

/**
 * The result object
 * @param graph Name of the output graph
 */
case class TriangleCountResult(graph: String)

/** Json conversion for arguments and return value case classes */
object TriangleCountJsonFormat {
  import DomainJsonProtocol._
  implicit val TCFormat = jsonFormat4(TriangleCountArgs)
  implicit val TCResultFormat = jsonFormat1(TriangleCountResult)
}

import TriangleCountJsonFormat._

/**
 * TriangleCount plugin implements the triangle count computation on a graph by invoking graphx TriangleCount.
 *
 * Pulls graph from underlying store, sends it off to the TriangleCountRunner, and then writes the output graph
 * back to the underlying store.
 *
 * Right now it is using only Titan for graph storage. Other backends including Parquet will be supported later.
 */
class TriangleCount extends SparkCommandPlugin[TriangleCountArgs, TriangleCountResult] {

  override def name: String = "graph:titan/ml/graphx_triangle_count"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def doc = Some(CommandDoc(oneLineSummary = "Triangle Count.",
    extendedSummary = Some("""
                             |    ** Experimental Feature **
                             |    Triangle Count.
                             |    Counts the number of triangles among vertices in an undirected graph.
                             |    If an edge is marked bidirectional, the implementation opts for canonical
                             |    orientation of edges hence counting it only once (as would have been for an
                             |    undirected graph)
                             |
                             |    Parameters
                             |    ----------
                             |    output_property : string
                             |        The name of output property to be added to vertex/edge upon completion
                             |    output_graph_name : string
                             |        The name of output graph to be created (original graph will be left unmodified)
                             |    input_edge_labels : list of string (optional)
                             |        The name of edge labels to be considered for triangle count
                             |        If None, all edges are considered
                             |
                             |    Returns
                             |    -------
                             |    graph: String
                             |        Name of the output graph. Call get_graph(graph) to get the handle to the new graph
                             |
                             |    Examples
                             |    --------
                             |        g.ml.graphx_triangle_count(output_property = "triangle_count",
                             |                                   output_graph_name = "tc_graph")
                             |
                             |    The expected output is like this::
                             |
                             |        {u'graph': u'tc_graph'}
                             |
                             |    To query::
                             |
                             |        tc_graph = get_graph('tc_graph')
                             |        tc_graph.query.gremlin("g.V [0..4]")
                             |
                             |        {u'results':[{u'_id':4,u'_type':u'vertex',u'b':3603376,u'pr':0.967054,
                             |        u'titanPhysicalId':363016,u'triangle_count':1},{u'_id':8,u'_type':u'vertex',u'a':
                             |        3118124,u'pr':0.967054,u'titanPhysicalId':343201000,u'triangle_count':1},
                             |        {u'_id':12,u'_type':u'vertex',u'a':3063711,u'pr':0.967054,u'titanPhysicalId':43068
                             |        ,u'triangle_count':1},{u'_id':16,u'_type':u'vertex',u'c':899225,u'pr':0.967054,
                             |        u'titanPhysicalId':202800088,u'triangle_count':1},{u'_id':20,u'_type':u'vertex',
                             |        u'c':1493990,u'pr':0.967054,u'titanPhysicalId':268188,u'triangle_count':1}],
                             |        u'run_time_seconds':0.271}
                             |
                           """.stripMargin)))

  override def execute(arguments: TriangleCountArgs)(implicit invocation: Invocation): TriangleCountResult = {

    sc.addJar(SparkContextFactory.jarPath("graphon"))

    // Titan Settings for input
    val config = configuration

    // Get the graph
    import scala.concurrent.duration._
    val graph = Await.result(engine.getGraph(arguments.graph.id), config.getInt("default-timeout") seconds)

    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)

    gbVertices.persist(StorageLevel.MEMORY_AND_DISK_SER)
    gbEdges.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val tcRunnerArgs = TriangleCountRunnerArgs(arguments.output_property,
      arguments.input_edge_labels)

    // Call TriangleCountRunner to kick off Triangle Count computation on RDDs
    val (outVertices, outEdges) = TriangleCountRunner.run(gbVertices, gbEdges, tcRunnerArgs)

    val newGraphName = arguments.output_graph_name
    val newGraph = Await.result(engine.createGraph(GraphTemplate(newGraphName, StorageFormats.HBaseTitan)),
      config.getInt("default-timeout") seconds)

    // create titan config copy for newGraph write-back
    val newTitanConfig = GraphBuilderConfigFactory.getTitanConfiguration(newGraph.name)
    writeToTitan(newTitanConfig, outVertices, outEdges)

    gbVertices.unpersist()
    gbEdges.unpersist()

    TriangleCountResult(newGraphName)
  }

  // Helper function to write rdds back to Titan
  private def writeToTitan(titanConfig: SerializableBaseConfiguration, gbVertices: RDD[GBVertex], gbEdges: RDD[GBEdge], append: Boolean = false) = {
    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append))
    gb.buildGraphWithSpark(gbVertices, gbEdges)
  }

}
