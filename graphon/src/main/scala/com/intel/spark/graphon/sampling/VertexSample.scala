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

import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.graph.GraphName
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphReference }
import org.apache.spark.SparkContext
import spray.json._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import com.intel.graphbuilder.elements.{ Edge, Vertex }
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.spark.graphon.GraphStatistics
import scala.util.Random
import scala.concurrent._
import java.util.UUID

import spray.json.DefaultJsonProtocol._
import scala.collection.JavaConverters._

/**
 * @param graph reference to the graph to be sampled
 * @param size the requested sample size
 * @param sampleType type of vertex sampling to use
 * @param seed random seed value
 */
case class VS(graph: GraphReference, size: Int, sampleType: String, seed: Int = 1)

case class VSResult(subgraph: GraphReference)

class VertexSample extends SparkCommandPlugin[VS, VSResult] {

  import DomainJsonProtocol._

  implicit val vsFormat = jsonFormat4(VS)
  implicit val vsResultFormat = jsonFormat1(VSResult)

  override def execute(invocation: SparkInvocation, arguments: VS)(implicit user: UserPrincipal, executionContext: ExecutionContext): VSResult = {

    // Titan Settings
    val config = configuration
    val titanConfigInput = config.getConfig("titan.load")

    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", titanConfigInput.getString("storage.backend"))
    titanConfig.setProperty("storage.hostname", titanConfigInput.getString("storage.hostname"))
    titanConfig.setProperty("storage.port", titanConfigInput.getString("storage.port"))

    import scala.concurrent.duration._
    val graph = Await.result(invocation.engine.getGraph(arguments.graph.id), config.getInt("default-timeout") seconds)

    val sc = invocation.sparkContext

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
    titanConfig.setProperty("storage.tablename", iatSubgraphName)

    val subgraph = Await.result(invocation.engine.createGraph(GraphTemplate(iatSubgraphName)), config.getInt("default-timeout") seconds)

    writeToTitan(vertexSample, edgeSample, titanConfig)

    VSResult(new GraphReference(subgraph.id))
  }

  /**
   * Produce a uniform vertex sample
   *
   * @param size the specified sample size
   * @param vertices the vertices to sample
   * @param seed random seed value
   * @return the vertices in the sample
   */
  def sampleVerticesUniform(vertices: RDD[Vertex], size: Int, seed: Int = 1): RDD[Vertex] = {
    require(size >= 1, "Invalid sample size: " + size)
    // TODO: Currently, all vertices are treated the same.  This should be extended to allow the user to specify, for example, different sample weights for different vertex types.
    if (size >= vertices.count()) {
      vertices
    }
    else {
      // currently without replacement
      val vertexSample = vertices.takeSample(false, size, seed)
      vertices.sparkContext.parallelize(vertexSample)
    }
  }

  /**
   * Produce a weighted vertex sample using the vertex degree as the weight
   *
   * This will result in a bias toward high-degree vertices.
   *
   * @param size the specified sample size
   * @param vertices the vertices to sample
   * @param edges RDD of all edges
   * @param seed random seed value
   * @return the vertices in the sample
   */
  def sampleVerticesDegree(vertices: RDD[Vertex], edges: RDD[Edge], size: Int, seed: Int = 1): RDD[Vertex] = {
    require(size >= 1, "Invalid sample size: " + size)
    if (size >= vertices.count()) {
      vertices
    }
    else {
      // NOTE: The current implementation here is for unordered sampling without replacement
      // create tuple of (vertexDegree, vertex)
      val vertexDegreeRdd = addVertexDegreeWeights(vertices, edges)
      // create tuple of (sampleWeight, vertex), where sampleWeight give degree-weighted probability of being sampled
      val weightedVertices = vertexDegreeRdd.map(pair => (Random.nextDouble() * pair._1, pair._2))
      //val vertexSampleArray = weightedVertices.top(size)(scala.math.Ordering[Double].on[(Double, Vertex)](pair => pair._1))
      val vertexSampleArray = weightedVertices.top(size)(Ordering.by(pair => pair._1))
      val vertexSamplePairRdd = vertices.sparkContext.parallelize(vertexSampleArray)
      vertexSamplePairRdd.map(pair => pair._2)
    }
  }

  /**
   * Produce a weighted vertex sample using the size of the degree histogram bin as the weight for a vertex, instead
   * of just the degree value itself.  This will result in a bias toward vertices with more frequent degree values.
   *
   * @param size the specified sample size
   * @param vertices the vertices to sample
   * @param edges RDD of all edges
   * @param seed random seed value
   * @return the vertices in the sample
   */
  def sampleVerticesDegreeDist(vertices: RDD[Vertex], edges: RDD[Edge], size: Int, seed: Int = 1): RDD[Vertex] = {
    require(size >= 1, "Invalid sample size: " + size)
    if (size >= vertices.count()) {
      vertices
    }
    else {
      // NOTE: The current implementation here is for unordered sampling without replacement
      // create tuple of (vertexDegree, vertex)
      val vertexDegreeRdd = addVertexDegreeDistWeights(vertices, edges)
      // create tuple of (sampleWeight, vertex), where sampleWeight give degree-weighted probability of being sampled
      val weightedVertices = vertexDegreeRdd.map(pair => (Random.nextDouble() * pair._1, pair._2))
      //val vertexSampleArray = weightedVertices.top(size)(scala.math.Ordering[Double].on[(Double, Vertex)](pair => pair._1))
      val vertexSampleArray = weightedVertices.top(size)(Ordering.by(pair => pair._1))
      val vertexSamplePairRdd = vertices.sparkContext.parallelize(vertexSampleArray)
      vertexSamplePairRdd.map(pair => pair._2)
    }
  }

  /**
   * Add the degree histogram bin size for each vertex as the degree weight.
   *
   * @param vertices RDD of all vertices
   * @param edges RDD of all edges
   * @return RDD of tuples that contain vertex weights
   */
  def addVertexDegreeDistWeights(vertices: RDD[Vertex], edges: RDD[Edge]): RDD[(Long, Vertex)] = {
    val vertexIdDegrees = GraphStatistics.outDegrees(edges)
    val vertexIds = vertices.map(vertex => (vertex.physicalId, vertex))
    val joinedRdd = vertexIdDegrees.join(vertexIds)
    val vertexDegreeRdd = joinedRdd.map(pair => pair._2) // tuples for (vertexDegree, Vertex)
    val degreeDistRdd = vertexDegreeRdd.groupBy(pair => pair._1).map(group => (group._1, group._2.size.toLong)) // tuples of (vertexDegreeDist, Vertex)
    degreeDistRdd.join(vertexDegreeRdd).map(pair => pair._2)
  }

  /**
   * Add the out-degree for each vertex as the degree weight.
   *
   * @param vertices RDD of all vertices
   * @param edges RDD of all edges
   * @return RDD of tuples that contain vertex weights
   */
  def addVertexDegreeWeights(vertices: RDD[Vertex], edges: RDD[Edge]): RDD[(Long, Vertex)] = {
    val vertexIdDegrees = GraphStatistics.outDegrees(edges)
    val vertexIds = vertices.map(vertex => (vertex.physicalId, vertex))
    val joinedRdd = vertexIdDegrees.join(vertexIds)
    val vertexDegreeRdd = joinedRdd.map(pair => pair._2)
    vertexDegreeRdd
  }

  /**
   * Gets the edges for the vertex induced subgraph
   *
   * @param vertices the set of sampled vertices to restrict the graph to
   * @param edges the full graph's set of edges
   * @return the edge set RDD for the vertex induced subgraph
   */
  def sampleEdges(vertices: RDD[Vertex], edges: RDD[Edge]): RDD[Edge] = {
    // TODO: graphX is welcome here...it has a subgraph function...and the current approach is inefficient
    val vertexArray = vertices.map(v => v.physicalId.asInstanceOf[Long]).collect()
    edges.filter(e => vertexArray.contains(e.headPhysicalId.asInstanceOf[Long]) && vertexArray.contains(e.tailPhysicalId.asInstanceOf[Long]))
  }

  /**
   * Read in the graph vertices and edges for the specified graphId
   *
   * @param graphName storage table name for the graph
   * @param sc access to SparkContext
   * @return tuple containing RDDs of vertices and edges
   */
  def getGraph(graphName: String, sc: SparkContext, titanConfig: SerializableBaseConfiguration): (RDD[Vertex], RDD[Edge]) = {
    titanConfig.setProperty("storage.tablename", graphName)

    val titanConnector = new TitanGraphConnector(titanConfig)

    // Read graph
    val titanReader = new TitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()

    val vertexRDD = titanReaderRDD.filterVertices()
    val edgeRDD = titanReaderRDD.filterEdges()

    (vertexRDD, edgeRDD)
  }

  /**
   * Write graph to Titan via GraphBuilder
   *
   * @param vertices the vertices to write to Titan
   * @param edges the edges to write to Titan
   */
  def writeToTitan(vertices: RDD[Vertex], edges: RDD[Edge], titanConfig: SerializableBaseConfiguration) = {
    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig))
    gb.buildGraphWithSpark(vertices, edges)
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
