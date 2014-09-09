package com.intel.spark.graphon.loopybeliefpropagation

import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }
import org.apache.spark.graphx.{ Graph, Edge }
import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext._

object LbpRunner {

  def runLbp(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], lbpParameters: Lbp): (RDD[GBVertex], RDD[GBEdge]) = {

    val outputPropertyLabel = lbpParameters.output_vertex_property_list.getOrElse("LBP_RESULT")

    // yeah, if youse try to do this with an empty RDD, then youse go fuck yourself... CAPICHE?

    val idPropertyName = inVertices.take(1)(0).gbId.key

    // convert to graphX vertices

    val graphXVertices =
      inVertices.map((gbVertex => (gbVertex.physicalId.asInstanceOf[Long], VertexState(Array(), gbVertex.id))))

    val graphXEdges = inEdges.map(edge =>
      (new Edge[Double](edge.tailPhysicalId.asInstanceOf[Long], edge.headPhysicalId.asInstanceOf[Long], 0)))

    val graph = Graph[VertexState, Double](graphXVertices, graphXEdges)

    val newGraph = GraphXLBP.runGraphXLBP(graph)

    val outVertices = newGraph.vertices.map({
      case (vid, vertexState) =>
        GBVertex(vertexState.id.asInstanceOf[Property].value, vertexState.id.asInstanceOf[Property], Seq(Property(outputPropertyLabel, vertexState.values)))
    })

    val outV: RDD[GBVertex] =
      inVertices.map(gbVertex => (gbVertex.id, gbVertex)).join(outVertices.map(gbVertex => (gbVertex.id, gbVertex))).map({ case (key: Any, (v1: GBVertex, v2: GBVertex)) => v1.merge(v2) })

    (outV, inEdges)
  }
}
