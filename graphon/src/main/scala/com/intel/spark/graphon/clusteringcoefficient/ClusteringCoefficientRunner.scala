package com.intel.spark.graphon.clusteringcoefficient

import com.intel.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import org.apache.spark.graphx.{ Edge => GraphXEdge, PartitionStrategy, Graph }
import org.apache.spark.graphx.lib.{ TriangleCount => GraphXTriangleCount }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

/**
 * Arguments for the TriangleCountRunnerArgs
 * @param outputEdgeLabel (optional) Name of the vertex property for storing local clustering coefficients.
 * @param inputEdgeSet List of edge labels to consider for clustering coefficient computation
 */
case class ClusteringCoefficientRunnerArgs(outputEdgeLabel: String,
                                           inputEdgeSet: Option[Set[String]])

/**
 * Provides a method for running triangle count on a graph using graphx. The result is a new graph with the
 * posterior value placed as a vertex property.
 */
object ClusteringCoefficientRunner extends Serializable {

  type EdgeSrcDestPair = (Long, Long)
  type GBVertexPropertyPair = (GBVertex, Property)
  type GBEdgePropertyPair = (GBEdge, Property)

  /**
   * Run clustering coefficient analysis of a graph.
   * @param inVertices Vertices of the incoming graph.
   * @param inEdges Edges of the incoming graph.
   * @param args Parameters controlling the execution of pagerank.
   * @return Vertices and edges for the output graph.
   */

  def run(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], args: ClusteringCoefficientRunnerArgs): (RDD[GBVertex], RDD[GBEdge]) = {

    val outputPropertyLabel = args.outputEdgeLabel
    val inputEdgeLabels = args.inputEdgeSet

    // Only select edges as specified in inputEdgeLabels
    val filteredEdges: RDD[GBEdge] = inputEdgeLabels match {
      case None => inEdges
      case _ => inEdges.filter(edge => inputEdgeLabels.get.contains(edge.label))
    }

    // convert to graphX vertices
    val graphXVertices: RDD[(Long, Null)] =
      inVertices.map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], null))

    val graphXEdges: RDD[GraphXEdge[Long]] = filteredEdges.map(edge => createGraphXEdgeFromGBEdge(edge))

    // create graphx Graph instance from graphx vertices and edges
    val graph = Graph[Null, Long](graphXVertices, graphXEdges)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    // run graphx trianglecount implementation
    val newGraph: Graph[Int, Long] = GraphXTriangleCount.run(graph)

    // extract vertices and edges from graphx graph instance
    val intermediateVertices: RDD[(Long, Property)] = newGraph.vertices.map({
      case (physicalId, triangleCount) => (physicalId, Property(outputPropertyLabel, triangleCount))
    })

    // Join the intermediate vertex/edge rdds with input vertex/edge rdd's to append the triangleCount attribute
    val outVertices: RDD[GBVertex] = inVertices
      .map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], gbVertex))
      .join(intermediateVertices)
      .map(vertex => generateGBVertex(vertex))

    (outVertices, inEdges)
  }

  // converts GBEdge to a GraphXEdge to be consumed by the triangle count computation
  // Note: GraphX Triangle count expects the edge to be in canonical orientation i.e. srcId < destId
  // Triangle Count makes sense on a undirected graph, hence we can change the edges to the canonical orientation
  // before passing it to graphx. We do not need the edges henceforth for join purposes.
  // Refer: https://spark.apache.org/docs/1.0.2/api/scala/index.html#org.apache.spark.graphx.lib.TriangleCount$
  private def createGraphXEdgeFromGBEdge(gbEdge: GBEdge, canonicalOrientation: Boolean = true): GraphXEdge[Long] = {
    val srcId = gbEdge.tailPhysicalId.asInstanceOf[Long]
    val destId = gbEdge.headPhysicalId.asInstanceOf[Long]
    if (canonicalOrientation && srcId > destId)
      GraphXEdge[Long](destId, srcId)
    else
      GraphXEdge[Long](srcId, destId)
  }

  // generates GBVertex from value pair obtained as a result of join and appends the pagerank property to the GBVertex
  private def generateGBVertex(joinValuePair: (Long, GBVertexPropertyPair)): GBVertex = {
    val (gbVertex, pagerankProperty) = joinValuePair._2 match {
      case value: GBVertexPropertyPair => (value._1, value._2)
    }
    gbVertex.copy(properties = gbVertex.properties + pagerankProperty)
  }

}