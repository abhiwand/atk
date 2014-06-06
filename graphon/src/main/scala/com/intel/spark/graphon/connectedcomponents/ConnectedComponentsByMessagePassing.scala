package com.intel.spark.graphon.connectedcomponents

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Edge => GraphXEdge}
/**
 * Implements the standard message-passing based connected components algorithm.
 *
 * Wraps the code straight from graphx/lib/connectedcomponents
 */

object ConnectedComponentsByMessagePassing {

  def run(vertexList: RDD[Long], edgeList: RDD[(Long, Long)]) = {

    val graphXVertices = vertexList.map((vid : Long) => (vid, null))
    val graphXEdges    = edgeList.map(edge =>   (new GraphXEdge(edge._1, edge._2)))

    val graph = Graph[Null,Nothing](graphXVertices, graphXEdges)

    val out : RDD[(Long, Long)]  = runGraphXCC(graph).vertices

    out
}

  /**
   * Compute the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   * @tparam ED the edge attribute type (preserved in the computation)
   *
   * @param graph the graph for which to compute the connected components
   *
   * @return a graph with vertex attributes containing the smallest vertex in each
   *         connected component
   */
  def runGraphXCC[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VertexId, ED] = {
    val ccGraph = graph.mapVertices { case (vid, _) => vid }
    def sendMessage(edge: EdgeTriplet[VertexId, ED]) = {
      if (edge.srcAttr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr))
      } else if (edge.srcAttr > edge.dstAttr) {
        Iterator((edge.srcId, edge.dstAttr))
      } else {
        Iterator.empty
      }
    }
    val initialMessage = Long.MaxValue
    Pregel(ccGraph, initialMessage, activeDirection = EdgeDirection.Either)(
      vprog = (id, attr, msg) => math.min(attr, msg),
      sendMsg = sendMessage,
      mergeMsg = (a, b) => math.min(a, b))
  } // end of connectedComponents

}

