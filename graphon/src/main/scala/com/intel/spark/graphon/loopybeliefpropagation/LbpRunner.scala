package com.intel.spark.graphon.loopybeliefpropagation

import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }
import org.apache.spark.graphx.{ Graph, Edge }
import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext._

object LbpRunner {

  def lbpVertexStateFromGBVertex(gbVertex: GBVertex, inputPropertyName: String): VertexState = {

    val prior = gbVertex.getProperty(inputPropertyName).get.value.asInstanceOf[List[Double]]

    val sum = prior.reduce(_ + _)
    val posterior = prior.map(x => x / sum)

    VertexState(gbVertex, prior, prior, posterior, 0.0d)

  }

  def outputGBVertrexfromLBPVertexState(vertexState: VertexState, outputPropertyLabel: String) = {
    val oldGBVertex = vertexState.gbVertex

    val properties = oldGBVertex.properties + Property(outputPropertyLabel, vertexState.posterior)

    GBVertex(oldGBVertex.physicalId, oldGBVertex.gbId, properties)
  }

  def runLbp(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], lbpParameters: Lbp): (RDD[GBVertex], RDD[GBEdge], String) = {

    val outputPropertyLabel = lbpParameters.output_vertex_property_list.getOrElse("LBP_RESULT")

    val inputPropertyName: String = lbpParameters.vertex_value_property_list.get

    val maxIterations: Int = lbpParameters.max_supersteps.getOrElse(20) // 20, really... 20?

    // convert to graphX vertices

    val graphXVertices: RDD[(Long, VertexState)] =
      inVertices.map((gbVertex => (gbVertex.physicalId.asInstanceOf[Long],
        lbpVertexStateFromGBVertex(gbVertex, inputPropertyName))))

    val graphXEdges = inEdges.map(edge =>
      (new Edge[Double](edge.tailPhysicalId.asInstanceOf[Long], edge.headPhysicalId.asInstanceOf[Long], 1)))

    val testGraphXVerticesIn = graphXVertices.collect()

    val graph = Graph[VertexState, Double](graphXVertices, graphXEdges)

    val (newGraph, log) = GraphXLBP.runGraphXLBP(graph, maxIterations, 2) // NLS TODO: state space hardwired to {0,1} !!

    // we need to get the correct value out of the posterior

    val outVertices = newGraph.vertices.map({
      case (vid, vertexState) =>
        outputGBVertrexfromLBPVertexState(vertexState, outputPropertyLabel)
    })

    (outVertices, inEdges, log)
  }
}
