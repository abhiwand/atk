package com.intel.spark.graphon.clusteringcoefficient

import com.intel.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import com.intel.spark.graphon.graphconversions.GraphConversions
import org.apache.spark.graphx.lib.ia.plugins.ClusteringCoefficient
import org.apache.spark.graphx.{ Edge => GraphXEdge, PartitionStrategy, Graph }
import org.apache.spark.graphx.lib.{ TriangleCount => GraphXTriangleCount }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

/**
 * Return value for the clustering coefficient runner
 * @param vertices RDD of vertices with the local clustering coefficient placed in the specified property.
 * @param globalClusteringCoefficient The global clustering coefficient of the input graph.
 */
case class ClusteringCoefficientRunnerReturn(vertices: RDD[GBVertex], globalClusteringCoefficient: Double)

/**
 * Provides a method for running clustering coefficient on a graph using graphx. The result is a new graph with the
 * local clustering coefficient placed as a vertex property.
 */
object ClusteringCoefficientRunner extends Serializable {

  /**
   * Run clustering coefficient analysis of a graph.
   * @param inVertices Vertices of the incoming graph.
   * @param inEdges Edges of the incoming graph.
   * @param outputPropertyLabel Optional name of the vertex property for storing local clustering coefficients.
   * @param inputEdgeLabels Optional list of edge labels to consider for clustering coefficient computation.
   * @return Vertices and edges for the output graph.
   */

  def run(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], outputPropertyLabel: Option[String], inputEdgeLabels: Option[Set[String]]): ClusteringCoefficientRunnerReturn = {

    // clustering coefficient is an undirected graph algorithm, so the input graph should
    // have the directed edge (b,a) present whenever the directed edge (a,b) is present... furthermore,
    // graphx expects one edge to be present ... from Min(a,b) to Max(a,b)
    val canonicalEdges: RDD[GBEdge] =
      inEdges.filter(gbEdge => (gbEdge.tailPhysicalId.asInstanceOf[Long] < gbEdge.headPhysicalId.asInstanceOf[Long]))

    val filteredEdges: RDD[GBEdge] = if (inputEdgeLabels.isEmpty) {
      canonicalEdges
    }
    else {
      canonicalEdges.filter(edge => inputEdgeLabels.get.contains(edge.label))
    }

    // convert to graphX vertices
    val graphXVertices: RDD[(Long, Null)] =
      inVertices.map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], null))

    val graphXEdges: RDD[GraphXEdge[Long]] = filteredEdges.map(edge => GraphConversions.createGraphXEdgeFromGBEdge(edge))

    // create graphx Graph instance from graphx vertices and edges
    val graph = Graph[Null, Long](graphXVertices, graphXEdges)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    // run graphx clustering coefficient implementation

    val (newGraph, globalClusteringCoefficient) = ClusteringCoefficient.run(graph)

    val outVertices = if (outputPropertyLabel.nonEmpty) {
      val outputProperty = outputPropertyLabel.get
      // extract vertices and edges from graphx graph instance
      val intermediateVertices: RDD[(Long, Property)] = newGraph.vertices.map({
        case (physicalId, clusteringCoefficient) => (physicalId, Property(outputProperty, clusteringCoefficient))
      })

      // Join the intermediate vertex/edge rdds with input vertex/edge rdd's to append the triangleCount attribute
      inVertices
        .map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], gbVertex))
        .join(intermediateVertices)
        .map({ case (_, (vertex, property)) => GraphConversions.addPropertyToVertex(property, vertex) })
    }
    else {
      inVertices
    }

    ClusteringCoefficientRunnerReturn(outVertices, globalClusteringCoefficient)
  }

}