package com.intel.spark.graphon.loopybeliefpropagation

import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }
import org.apache.spark.graphx.{ Graph, Edge }
import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext._

object LbpRunner {

  def runLbp(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], lbpParameters: Lbp): (RDD[GBVertex], RDD[GBEdge], String) = {

    val outputPropertyLabel = lbpParameters.output_vertex_property_list.getOrElse("LBP_RESULT")

    val inputPropertyName: String = lbpParameters.vertex_value_property_list.get

    val maxIterations: Int = lbpParameters.max_supersteps.getOrElse(20) // 20, really... 20?

    // convert to graphX vertices

    val graphXVertices =
      inVertices.map((gbVertex => (gbVertex.physicalId.asInstanceOf[Long],
        VertexState(gbVertex.getProperty(inputPropertyName).get.value.asInstanceOf[Double],
          gbVertex.id, 0))))

    val graphXEdges = inEdges.map(edge =>
      (new Edge[Double](edge.tailPhysicalId.asInstanceOf[Long], edge.headPhysicalId.asInstanceOf[Long], 0)))

    val graph = Graph[VertexState, Double](graphXVertices, graphXEdges)

    val (newGraph, log) = GraphXLBP.runGraphXLBP(graph, maxIterations)

    val outVertices = newGraph.vertices.map({
      case (vid, vertexState) =>
        GBVertex(vertexState.id.asInstanceOf[Property].value, vertexState.id.asInstanceOf[Property], Set(Property(outputPropertyLabel, vertexState.value)))
    })

    // the trade-off:
    // either we pass along all of the data into GraphX or we do a join at the end...
    // the join is pretty expensive, and the data has to live somewhere anyway
    val outV: RDD[GBVertex] =
      inVertices.map(gbVertex => (gbVertex.id, gbVertex)).join(outVertices.map(gbVertex => (gbVertex.id, gbVertex))).map({ case (key: Any, (v1: GBVertex, v2: GBVertex)) => v1.merge(v2) })

    (outV, inEdges, log)
  }
}
