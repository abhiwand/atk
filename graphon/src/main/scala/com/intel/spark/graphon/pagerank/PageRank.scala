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

package com.intel.spark.graphon.pagerank

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphReference }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.{ Await, ExecutionContext }
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.graph.GraphName
import spray.json._
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Vertex => GBVertex, Edge => GBEdge }
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
                        convergence_tolerance: Option[Double] = None)

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

  override def doc = Some(CommandDoc(oneLineSummary = "Page Rank.",
    extendedSummary = Some("""
                             |    The `PageRank algorithm <http://en.wikipedia.org/wiki/PageRank>`_.
                             |
                             |    Parameters
                             |    ----------
                             |    output_property : string
                             |        The name of output property to be added to vertex/edge upon completion
                             |    output_graph_name : string
                             |        The name of output graph to be created (original graph will be left unmodified)
                             |    input_edge_labels : list of string (optional)
                             |        The name of edge labels to be considered for pagerank
                             |        If None, all edges are considered
                             |    max_iterations : integer (optional)
                             |        The maximum number of iterations that the algorithm will execute.
                             |        The valid value range is all positive integer.
                             |        The default value is 20.
                             |    convergence_tolerance : float (optional)
                             |        The amount of change in cost function that will be tolerated at
                             |        convergence. If this parameter is specified, max_iterations is no longer
                             |        considered as a stopping condition.
                             |        If the change is less than this threshold, the algorithm exits earlier.
                             |        The valid value range is all Float and zero.
                             |        The default value is 0.001.
                             |    reset_probability : float (optional)
                             |        The probability that the random walk of a page is reset.
                             |        The default value is 0.15.
                             |
                             |    Returns
                             |    -------
                             |    graph: String
                             |        Name of the output graph. Call get_graph(graph) to get the handle to the new graph
                             |
                             |    Examples
                             |    --------
                             |        g.ml.graphx_pagerank(output_property = "pr_result", output_graph_name = "pr_graph")
                             |
                             |    The expected output is like this::
                             |
                             |        {u'graph': u'pr_graph'}
                             |
                             |    To query::
                             |
                             |        pr_graph = get_graph('pr_graph')
                             |        pr_graph.query.gremlin("g.V [0..4]")
                             |
                             |        {u'results':[{u'_id':4,u'_type':u'vertex',u'pr_result':0.787226,
                             |        u'titanPhysicalId':133200148,u'user_id':7665,u'vertex_type':u'L'},{u'_id':8,
                             |        u'_type':u'vertex',u'pr_result':1.284043,u'movie_id':7080,u'titanPhysicalId':85200356,
                             |        u'vertex_type':u'R'},{u'_id':12,u'_type':u'vertex',u'pr_result':0.186911,
                             |        u'movie_id':8904,u'titanPhysicalId':15600404,u'vertex_type':u'R'},{u'_id':16,
                             |        u'_type':u'vertex',u'pr_result':0.384138,u'movie_id':6836,u'titanPhysicalId':105600396,
                             |        u'vertex_type': u'R'},{u'_id':20,u'_type':u'vertex',u'pr_result':0.822977,
                             |        u'titanPhysicalId':68400136,u'user_id':3223,u'vertex_type':u'L'}],
                             |        u'run_time_seconds':1.489}
                             |
                             |
                           """.stripMargin)))

  override def execute(sparkInvocation: SparkInvocation, arguments: PageRankArgs)(implicit user: UserPrincipal, executionContext: ExecutionContext): PageRankResult = {

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

      val gbVertices: RDD[GBVertex] = titanReaderRDD.filterVertices()
      val gbEdges: RDD[GBEdge] = titanReaderRDD.filterEdges()

      val prRunnerArgs = PageRankRunnerArgs(arguments.output_property,
        arguments.input_edge_labels,
        arguments.max_iterations,
        arguments.reset_probability,
        arguments.convergence_tolerance)

      // Call PageRankRunner to kick off PageRank computation on RDDs
      val (outVertices, outEdges) = PageRankRunner.run(gbVertices, gbEdges, prRunnerArgs)

      val newGraphName = arguments.output_graph_name
      val iatNewGraphName = GraphName.convertGraphUserNameToBackendName(newGraphName)
      val newGraph = Await.result(sparkInvocation.engine.createGraph(GraphTemplate(newGraphName)),
        config.getInt("default-timeout") seconds)

      // create titan config copy for newGraph write-back
      val newTitanConfig = new SerializableBaseConfiguration()
      newTitanConfig.copy(titanConfig)
      newTitanConfig.setProperty("storage.tablename", iatNewGraphName)
      writeToTitan(newTitanConfig, outVertices, outEdges)

      PageRankResult(newGraphName)
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

