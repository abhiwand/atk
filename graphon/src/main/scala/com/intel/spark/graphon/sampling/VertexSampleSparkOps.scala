//////////////////////////////////////////////////////////////////////////////
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
//////////////////////////////////////////////////////////////////////////////

package com.intel.spark.graphon.sampling

import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.intel.graphbuilder.elements.{ Edge, Vertex }
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.spark.graphon.GraphStatistics
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import java.util.Random // scala.util.Random is not serializable ???

object VertexSampleSparkOps extends Serializable {

  /**
   * Produce a uniform vertex sample
   *
   * @param vertices the vertices to sample
   * @param size the specified sample size
   * @param seed optional random seed value
   * @return RDD containing the vertices in the sample
   */
  def sampleVerticesUniform(vertices: RDD[Vertex], size: Int, seed: Option[Long]): RDD[Vertex] = {
    require(size >= 1, "Invalid sample size: " + size)
    // TODO: Currently, all vertices are treated the same.  This should be extended to allow the user to specify, for example, different sample weights for different vertex types.
    if (size >= vertices.count()) {
      vertices
    }
    else {
      val rand = checkSeed(seed)

      // create tuple of (sampleWeight, vertex)
      val weightedVertexRdd = vertices.map(vertex => (rand.nextDouble(), vertex))

      getTopVertices(weightedVertexRdd, size)
    }
  }

  /**
   * Produce a weighted vertex sample using the vertex degree as the weight
   *
   * This will result in a bias toward high-degree vertices.
   *
   * @param vertices the vertices to sample
   * @param edges RDD of all edges
   * @param size the specified sample size
   * @param seed optional random seed value
   * @return RDD containing the vertices in the sample
   */
  def sampleVerticesDegree(vertices: RDD[Vertex], edges: RDD[Edge], size: Int, seed: Option[Long]): RDD[Vertex] = {
    require(size >= 1, "Invalid sample size: " + size)
    if (size >= vertices.count()) {
      vertices
    }
    else {
      val rand = checkSeed(seed)

      // create tuple of (vertexDegree, vertex)
      val vertexDegreeRdd = addVertexDegreeWeights(vertices, edges)

      // create tuple of (sampleWeight, vertex)
      val weightedVertexRdd = vertexDegreeRdd.map{ case(vertexDegree, vertex) => (rand.nextDouble() * vertexDegree, vertex) }

      getTopVertices(weightedVertexRdd, size)
    }
  }

  /**
   * Produce a weighted vertex sample using the size of the degree histogram bin as the weight for a vertex, instead
   * of just the degree value itself
   *
   * This will result in a bias toward vertices with more frequent degree values.
   *
   * @param vertices the vertices to sample
   * @param edges RDD of all edges
   * @param size the specified sample size
   * @param seed optional random seed value
   * @return RDD containing the vertices in the sample
   */
  def sampleVerticesDegreeDist(vertices: RDD[Vertex], edges: RDD[Edge], size: Int, seed: Option[Long]): RDD[Vertex] = {
    require(size >= 1, "Invalid sample size: " + size)
    if (size >= vertices.count()) {
      vertices
    }
    else {
      val rand = checkSeed(seed)

      // create tuple of (vertexDegreeBinSize, vertex)
      val vertexDegreeRdd = addVertexDegreeDistWeights(vertices, edges)

      // create tuple of (sampleWeight, vertex)
      val weightedVertexRdd = vertexDegreeRdd.map{ case(vertexDegreeBinSize, vertex) => (rand.nextDouble() * vertexDegreeBinSize, vertex) }

      getTopVertices(weightedVertexRdd, size)
    }
  }

  /**
   * Get the edges for the vertex induced subgraph
   *
   * @param vertices the set of sampled vertices from which to construct the vertex induced subgraph
   * @param edges the set of edges for the input graph
   * @return the edge RDD for the vertex induced subgraph
   */
  def vertexInducedEdgeSet(vertices: RDD[Vertex], edges: RDD[Edge]): RDD[Edge] = {
    // TODO: Find more efficient way of doing this that does not involve collecting sampled vertices
    val vertexArray = vertices.map(v => v.physicalId).collect()
    edges.filter(e => vertexArray.contains(e.headPhysicalId) && vertexArray.contains(e.tailPhysicalId))
  }

  /**
   * Read in the graph vertices and edges for the specified graphName
   *
   * @param sc access to SparkContext
   * @param titanConfig the config for Titan
   * @return tuple containing RDDs of vertices and edges
   */
  def getGraphRdds(sc: SparkContext, titanConfig: SerializableBaseConfiguration): (RDD[Vertex], RDD[Edge]) = {
    val titanConnector = new TitanGraphConnector(titanConfig)

    // read graph
    val titanReader = new TitanReader(sc, titanConnector)
    val titanReaderRDD = titanReader.read()

    // filter the vertices and edges
    val vertexRDD = titanReaderRDD.filterVertices()
    val edgeRDD = titanReaderRDD.filterEdges()

    (vertexRDD, edgeRDD)
  }

  /**
   * Write graph to Titan via GraphBuilder
   *
   * @param vertices the vertices to write to Titan
   * @param edges the edges to write to Titan
   * @param titanConfig the config for Titan
   */
  def writeToTitan(vertices: RDD[Vertex], edges: RDD[Edge], titanConfig: SerializableBaseConfiguration) = {
    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig))
    gb.buildGraphWithSpark(vertices, edges)
  }

  /**
   * Check the optional seed and set if necessary
   *
   * @param seed optional random seed
   * @return a new Random instance
   */
  private def checkSeed(seed: Option[Long]): Random = {
    seed match {
      case Some(l) => new Random(l)
      case _ => new Random
    }
  }

  /**
   * Get the top *size* number of vertices sorted by weight
   *
   * @param weightedVertexRdd RDD of (vertexWeight, vertex)
   * @param size number of vertices to take
   * @return RDD of vertices
   */
  private def getTopVertices(weightedVertexRdd: RDD[(Double, Vertex)], size: Int): RDD[Vertex] = {
    val vertexSampleArray = weightedVertexRdd.top(size)(Ordering.by { case (vertexWeight, vertex) => vertexWeight })
    val vertexSamplePairRdd = weightedVertexRdd.sparkContext.parallelize(vertexSampleArray)

    vertexSamplePairRdd.map{ case(vertexWeight, vertex) => vertex }
  }

  /**
   * Add the degree histogram bin size for each vertex as the degree weight
   *
   * @param vertices RDD of all vertices
   * @param edges RDD of all edges
   * @return RDD of tuples that contain vertex degree histogram bin size as weight for each vertex
   */
  private def addVertexDegreeDistWeights(vertices: RDD[Vertex], edges: RDD[Edge]): RDD[(Long, Vertex)] = {
    // get tuples of (vertexDegree, vertex)
    val vertexDegreeRdd = addVertexDegreeWeights(vertices, edges)

    // get tuples of (vertexDegreeBinSize, vertex)
    val groupedByBinSizeRdd = vertexDegreeRdd.groupBy{ case(vertexDegreeBinSize, vertex) => vertexDegreeBinSize }
    val degreeDistRdd = groupedByBinSizeRdd.map{ case(vertexDegreeBinSize, vertexSeq) => (vertexDegreeBinSize, vertexSeq.size.toLong) }

    degreeDistRdd.join(vertexDegreeRdd).map{ case(vertexDegreeBinSize, degreeVertexPair) => degreeVertexPair }
  }

  /**
   * Add the out-degree for each vertex as the degree weight
   *
   * @param vertices RDD of all vertices
   * @param edges RDD of all edges
   * @return RDD of tuples that contain vertex degree as weight for each vertex
   */
  private def addVertexDegreeWeights(vertices: RDD[Vertex], edges: RDD[Edge]): RDD[(Long, Vertex)] = {
    val vertexIdDegrees = GraphStatistics.outDegrees(edges)
    val vertexIds = vertices.map(vertex => (vertex.physicalId, vertex))

    val degreeVertexPairsRdd = vertexIdDegrees.join(vertexIds)

    degreeVertexPairsRdd.map { case (degree, degreeVertexPair) => degreeVertexPair }
  }

}
