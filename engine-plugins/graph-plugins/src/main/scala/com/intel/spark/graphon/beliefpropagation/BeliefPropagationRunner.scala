//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.spark.graphon.beliefpropagation

import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import org.apache.spark.graphx.{ PartitionStrategy, Graph, Edge }
import com.intel.intelanalytics._
import com.intel.spark.graphon.VectorMath

/**
 * Arguments for the BeliefPropagationRunner
 * @param posteriorProperty Name of the property to which the posteriors will be written.
 * @param priorProperty Name of the property containing the priors.
 * @param maxIterations Maximum number of iteratiosn to execute BP message passing.
 * @param stringOutput When true, the output is a comma-delimited string, when false (default) the output is a vector.
 * @param convergenceThreshold Optional Double. BP will terminate when average change in posterior beliefs between
 *                             supersteps is less than or equal to this threshold. Defaults to 0.
 * @param edgeWeightProperty Optional. Property containing edge weights.
 */
case class BeliefPropagationRunnerArgs(posteriorProperty: String,
                                       priorProperty: String,
                                       maxIterations: Option[Int],
                                       stringOutput: Option[Boolean],
                                       convergenceThreshold: Option[Double],
                                       edgeWeightProperty: Option[String])
/**
 * Provides a method for running belief propagation on a graph. The result is a new graph with the belief-propagation
 * posterior beliefs placed in a new vertex property on each vertex.
 */
object BeliefPropagationRunner extends Serializable {

  val separators: Array[Char] = Array(' ', ',', '\t')

  /**
   * Run belief propagation on a graph.
   * @param inVertices Vertices of the incoming graph.
   * @param inEdges Edges of the incoming graph.
   * @param args Parameters controlling the execution of belief propagation.
   * @return Vertex and edge list for the output graph and a logging string reporting on the execution of the belief
   *         propagation run.
   */

  def run(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], args: BeliefPropagationRunnerArgs): (RDD[GBVertex], RDD[GBEdge], String) = {

    val outputPropertyLabel = args.posteriorProperty
    val inputPropertyName: String = args.priorProperty
    val maxIterations: Int = args.maxIterations.getOrElse(BeliefPropagationDefaults.maxIterationsDefault)
    val beliefsAsStrings = args.stringOutput.getOrElse(BeliefPropagationDefaults.stringOutputDefault)
    val convergenceThreshold = args.convergenceThreshold.getOrElse(BeliefPropagationDefaults.convergenceThreshold)

    val firstVertexOption: Option[GBVertex] = try {
      Some(inVertices.first())
    }
    catch {
      case e: UnsupportedOperationException => None
    }

    if (firstVertexOption.isEmpty) {
      (inVertices, inEdges, "Attempt to run belief propagation on a vertex free graph. No output.")
    }
    else {

      val firstVertex = firstVertexOption.get
      val firstPropertyOption = firstVertexOption.get.getProperty(inputPropertyName)

      if (firstPropertyOption.isEmpty) {
        throw new NotFoundException("Vertex Property", inputPropertyName,
          "Vertex ID ==" + firstVertex.gbId.value.toString + "    Physical ID == " + firstVertex.physicalId)
      }
      else {
        val firstPrior = firstPropertyOption.get.value

        val stateSpaceSize: Int = firstPrior match {
          case v: Vector[_] => v.length
          case s: String => s.split(separators).filter(_.nonEmpty).map(_.toDouble).toVector.length
        }

        val defaultEdgeWeight = BeliefPropagationDefaults.edgeWeightDefault

        val power = BeliefPropagationDefaults.powerDefault
        val smoothing = BeliefPropagationDefaults.smoothingDefault

        // convert to graphX vertices

        val graphXVertices: RDD[(Long, VertexState)] =
          inVertices.map(gbVertex => gbVertex.physicalId match {
            case a: Any => (gbVertex.physicalId.asInstanceOf[Long], bpVertexStateFromVertex(gbVertex, inputPropertyName, stateSpaceSize))
            case null => (gbVertex.gbId.value.asInstanceOf[Long], bpVertexStateFromVertex(gbVertex, inputPropertyName, stateSpaceSize))
          })
        //          } (gbVertex.physicalId.asInstanceOf[Long],
        //            bpVertexStateFromVertex(gbVertex, inputPropertyName, stateSpaceSize)))

        val graphXEdges = inEdges.map(edge => bpEdgeStateFromEdge(edge, args.edgeWeightProperty, defaultEdgeWeight))

        val graph = Graph[VertexState, Double](graphXVertices, graphXEdges)
          .partitionBy(PartitionStrategy.RandomVertexCut)

        val graphXLBPRunner = new PregelBeliefPropagation(maxIterations, power, smoothing, convergenceThreshold)
        val (newGraph, log) = graphXLBPRunner.run(graph)

        val outVertices = newGraph.vertices.map({
          case (vid, vertexState) =>
            vertexFromBPVertexState(vertexState, outputPropertyLabel, beliefsAsStrings)
        })

        (outVertices, inEdges, log)
      }
    }
  }

  // converts incoming edge to the form consumed by the belief propagation computation
  private def bpEdgeStateFromEdge(gbEdge: GBEdge, edgeWeightPropertyNameOption: Option[String], defaultEdgeWeight: Double) = {

    val weight: Double = if (edgeWeightPropertyNameOption.nonEmpty) {

      val edgeWeightPropertyName = edgeWeightPropertyNameOption.get

      val property = gbEdge.getProperty(edgeWeightPropertyName)

      if (property.isEmpty) {
        throw new NotFoundException("Edge Property", edgeWeightPropertyName, "Edge ID == " + gbEdge.id + "\n"
          + "Source Vertex == " + gbEdge.tailVertexGbId.value + "\n"
          + "Destination Vertex == " + gbEdge.headVertexGbId.value)
      }
      else {
        gbEdge.getProperty(edgeWeightPropertyNameOption.get).get.asInstanceOf[Double]
      }
    }
    else {
      defaultEdgeWeight
    }
    val srcId =
      gbEdge.tailPhysicalId match {
        case a: Any => gbEdge.tailPhysicalId.asInstanceOf[Long]
        case null => gbEdge.tailVertexGbId.value.asInstanceOf[Long]
      }

    val destId =
      gbEdge.headPhysicalId match {
        case a: Any => gbEdge.headPhysicalId.asInstanceOf[Long]
        case null => gbEdge.headVertexGbId.value.asInstanceOf[Long]
      }
    new Edge[Double](srcId, destId, weight)
  }

  // converts incoming vertex to the form consumed by the belief propagation computation
  private def bpVertexStateFromVertex(gbVertex: GBVertex,
                                      inputPropertyName: String, stateSpaceSize: Int): VertexState = {

    val property = gbVertex.getProperty(inputPropertyName)

    val prior: Vector[Double] = if (property.isEmpty) {
      throw new NotFoundException("Vertex Property", inputPropertyName,
        "Vertex ID ==" + gbVertex.gbId.value.toString + "    Physical ID == " + gbVertex.physicalId)
    }
    else {
      property.get.value match {
        case v: Vector[_] => v.asInstanceOf[Vector[Double]]
        case s: String => s.split(separators).filter(_.nonEmpty).map(_.toDouble).toVector
      }
    }

    if (prior.length != stateSpaceSize) {
      throw new IllegalArgumentException("Length of prior does not match state space size\n" +
        "Vertex ID ==" + gbVertex.gbId.value.toString + "    Physical ID == " + gbVertex.physicalId + "\n" +
        "Property name == " + inputPropertyName + "    Expected state space size " + stateSpaceSize)
    }
    val posterior = VectorMath.l1Normalize(prior)

    VertexState(gbVertex, messages = Map(), prior, posterior, delta = 0)

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
