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

package com.intel.spark.graphon.pagerank

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
import com.intel.graphbuilder.elements.{ GBVertex, GBEdge }
import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.intelanalytics.domain.command.CommandDoc
import org.apache.spark.{ SparkConf, SparkContext }
import java.util.UUID

/**
 * Parameters for executing page rank.
 * @param graph Reference to the graph object on which to compute pagerank.
 * @param output_property Name of the property to which pagerank value will be stored on vertex and edge.
 * @param output_graph_name Name of output graph.
 * @param input_edge_labels List of edge labels to consider for pagerank computation. If None, all edges are considered.
 * @param max_iterations Optional Integer. The maximum number of iterations that will be invoked. Defaults to 20.
 * @param reset_probability Optional Double. Random reset probability
 * @param convergence_tolerance Optional Double. Tolerance allowed at convergence
 *                             (smaller values tend to yield accurate results)
 */
case class PageRankArgs(graph: GraphReference,
                        output_property: String,
                        output_graph_name: String,
                        input_edge_labels: Option[List[String]] = None,
                        max_iterations: Option[Int] = None,
                        reset_probability: Option[Double] = None,
                        convergence_tolerance: Option[Double] = None) {
  require(!output_property.isEmpty, "Output property label must be provided")
  require(!output_graph_name.isEmpty, "Output graph name must be provided")
}

/**
 * Companion object holds the default values.
 */
object PageRankDefaults {
  val maxIterationsDefault = 20
  val resetProbabilityDefault = 0.15d
  val convergenceToleranceDefault = 0.001d
}

/**
 * The result object
 * @param graph Name of the output graph
 */
case class PageRankResult(graph: String)

/** Json conversion for arguments and return value case classes */
object PageRankJsonFormat {
  import DomainJsonProtocol._
  implicit val PRFormat = jsonFormat7(PageRankArgs)
  implicit val PRResultFormat = jsonFormat1(PageRankResult)
}

import PageRankJsonFormat._

/**
 * PageRank plugin implements the pagerank computation on a graph by invoking graphx pagerank.
 *
 * Pulls graph from underlying store, sends it off to the PageRankRunner, and then writes the output graph
 * back to the underlying store.
 *
 * Right now it is using only Titan for graph storage. Other backends including Parquet will be supported later.
 */
class PageRank extends SparkCommandPlugin[PageRankArgs, PageRankResult] {

  override def name: String = "graph:titan/ml/graphx_pagerank"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: PageRankArgs)(implicit invocation: Invocation): PageRankResult = {

    sc.addJar(SparkContextFactory.jarPath("graphon"))

    // Titan Settings for input
    val config = configuration

    // Get the graph
    val graph = engine.graphs.expectGraph(arguments.graph)

    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)

    val prRunnerArgs = PageRankRunnerArgs(arguments.output_property,
      arguments.input_edge_labels,
      arguments.max_iterations,
      arguments.reset_probability,
      arguments.convergence_tolerance)

    // Call PageRankRunner to kick off PageRank computation on RDDs
    val (outVertices, outEdges) = PageRankRunner.run(gbVertices, gbEdges, prRunnerArgs)

    val newGraphName = arguments.output_graph_name
    val newGraph = engine.graphs.createGraph(GraphTemplate(Some(newGraphName), StorageFormats.HBaseTitan))

    // create titan config copy for newGraph write-back
    val newTitanConfig = GraphBuilderConfigFactory.getTitanConfiguration(newGraph)
    writeToTitan(newTitanConfig, outVertices, outEdges)

    PageRankResult(newGraphName)
  }

  // Helper function to write rdds back to Titan
  private def writeToTitan(titanConfig: SerializableBaseConfiguration, gbVertices: RDD[GBVertex], gbEdges: RDD[GBEdge], append: Boolean = false) = {
    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append))
    gb.buildGraphWithSpark(gbVertices, gbEdges)
  }

}
