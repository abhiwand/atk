package com.intel.spark.graphon.loopybeliefpropagation

import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }
import org.apache.spark.graphx.{ Graph, Edge }
import com.intel.intelanalytics._
import com.intel.spark.graphon.VectorMath

/**
 * Provides a method for running belief propagation on a graph. The result is a new graph with the belief-propagation
 * posterior beliefs placed in a new vertex property on each vertex.
 */
object LbpRunner {

  /**
   * Run belief propagation on a graph.
   * @param inVertices Vertices of the incoming graph.
   * @param inEdges Edges of the incoming graph.
   * @param lbpParameters Parameters controlling the execution of belief propagation.
   * @return Vertex and edge list for the output graph and a logging string reporting on the execution of the belief
   *         propagation run.
   */

  def runLbp(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], lbpParameters: Lbp): (RDD[GBVertex], RDD[GBEdge], String) = {

    val defaultMaxIterations = 20
    val defaultEdgeWeight = 1.0d
    val defaultSizeOfStateSpace = 2

    val outputPropertyLabel = lbpParameters.posteriorPropertyName
    val inputPropertyName: String = lbpParameters.vertexPriorPropertyName
    val maxIterations: Int = lbpParameters.maxSuperSteps.getOrElse(defaultMaxIterations)
    val beliefsAsStrings = lbpParameters.beliefsAsStrings

    // convert to graphX vertices

    val graphXVertices: RDD[(Long, VertexState)] =
      inVertices.map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long],
        bpVertexStateFromVertex(gbVertex, inputPropertyName)))

    val graphXEdges = inEdges.map(edge =>
      new Edge[Double](edge.tailPhysicalId.asInstanceOf[Long], edge.headPhysicalId.asInstanceOf[Long], defaultEdgeWeight))

    val graph = Graph[VertexState, Double](graphXVertices, graphXEdges)

    val graphXLBPRunner = new GraphXLBP(maxIterations, defaultSizeOfStateSpace)
    val (newGraph, log) = graphXLBPRunner.run(graph)

    val outVertices = newGraph.vertices.map({
      case (vid, vertexState) =>
        vertexFromBPVertexState(vertexState, outputPropertyLabel, beliefsAsStrings)
    })

    (outVertices, inEdges, log)
  }

  // converts incoming vertex to the form consumed by the belief propagation computation
  private def bpVertexStateFromVertex(gbVertex: GBVertex,
                                      inputPropertyName: String): VertexState = {

    val property = gbVertex.getProperty(inputPropertyName)

    val prior: Vector[Double] = if (property.isEmpty) {
      throw new NotFoundException("Vertex Property", inputPropertyName,
        "Vertex ID ==" + gbVertex.gbId.value.toString + "    Physical ID == " + gbVertex.physicalId)
    }
    else {
      property.get.value match {
        case v: Vector[Double] => v
        case s: String => s.split(",").map(x => x.toDouble).toVector
      }
    }

    val posterior = VectorMath.l1Normalize(prior)

    VertexState(gbVertex, messages = Map(), prior, posterior, delta = 0.0d)

  }

  // converts vertex in belief propagation output into the common graph representation for output
  private def vertexFromBPVertexState(vertexState: VertexState, outputPropertyLabel: String, beliefsAsStrings: Boolean) = {
    val oldGBVertex = vertexState.gbVertex

    val posteriorProperty: Property = if (beliefsAsStrings) {
      Property(outputPropertyLabel, vertexState.posterior.map(x => x.toString).mkString(", "))
    }
    else {
      Property(outputPropertyLabel, vertexState.posterior)
    }

    val properties = oldGBVertex.properties + posteriorProperty

    GBVertex(oldGBVertex.physicalId, oldGBVertex.gbId, properties)
  }
}
