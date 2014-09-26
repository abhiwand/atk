package com.intel.spark.graphon.loopybeliefpropagation

import org.apache.spark.graphx._
import com.intel.spark.graphon.iatpregel._
import com.intel.graphbuilder.elements.{ Vertex => GBVertex }
import com.intel.spark.graphon.VectorMath

/**
 * Internal state of a vertex during the progress of the belief propagation algorithm.
 * @param gbVertex The underlying vertex in the input graph. Used to propagate information out.
 * @param messages The messages that the vertex has received from its neighbors.
 * @param prior The prior probability distribution for this vertex.
 * @param posterior The current belief as informed by the latest round of message passing.
 * @param delta The difference between the new posterior belief and the last posterior belief.
 *              Used to gauge convergence.
 */
case class VertexState(gbVertex: GBVertex,
                       messages: Map[VertexId, Vector[Double]],
                       prior: Vector[Double],
                       posterior: Vector[Double],
                       delta: Double) extends DeltaProvider

/**
 * Provides a method to run belief propagation on a graph.
 * @param maxIterations Bound on the number of iterations.
 * @param stateSpaceSize The number of
 * @param power Exponent used in the potential function.
 * @param smoothing Smoothing parameter used in the potential function
 */
class GraphXLBP(val maxIterations: Int,
                val stateSpaceSize: Int,
                val power: Double = 1.0d,
                val smoothing: Double = 1.0d) extends Serializable {

  /**
   * Run belief propagation on a graph.
   *
   * @param graph GraphX graph to be analyzed.
   *
   * @return The graph with posterior probabilities updated by belief propagation, and a logging string
   *         reporting on the execution of the algorithm.
   */
  def run(graph: Graph[VertexState, Double]): (Graph[VertexState, Double], String) = {

    // choose loggers

    val initialReporter = new BasicCountsInitialReport[VertexState, Double]
    val superStepReporter = new AverageDeltaSuperStepReport[VertexState]

    // call  Pregel

    IATPregel(graph,
      Map().asInstanceOf[Map[Long, Vector[Double]]],
      initialReporter,
      superStepReporter,
      maxIterations = maxIterations,
      activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, mergeMsg)

  }

  /**
   * Pregel required method to update the state of a vertex from the messages it has received.
   * @param id The id of the currently processed vertex.
   * @param vertexState The state of the currently processed vertex.
   * @param messages A map of the (neighbor, message-from-neighbor) pairs for the most recent round of message passing.
   * @return New state of the vertex.
   */
  private def vertexProgram(id: VertexId, vertexState: VertexState, messages: Map[VertexId, Vector[Double]]): VertexState = {

    val prior = vertexState.prior

    val oldPosterior = vertexState.posterior

    val messageValues: List[Vector[Double]] = messages.toList.map({ case (k, v) => v })

    val productOfPriorAndMessages: Vector[Double] = VectorMath.overflowProtectedProduct(prior :: messageValues).get

    val posterior = VectorMath.l1Normalize(productOfPriorAndMessages)

    val delta = posterior.zip(oldPosterior).map({ case (x, y) => Math.abs(x - y) }).reduce(_ + _)

    VertexState(vertexState.gbVertex, messages, prior, posterior, delta)
  }

  /**
   * The edge potential function provides an estimate of how compatible the states are between two joined vertices.
   * This is the one inspired by the Boltzmann distribution.
   * @param delta Difference in the states between given vertices.
   * @param weight Edge weight.
   * @return Compatibility estimate for the two states..
   */
  private def edgePotential(delta: Double, weight: Double) = {
    -Math.pow(delta.toDouble, power) * weight * smoothing
  }

  /**
   * Calculates the message to be sent from one vertex to another.
   * @param sender ID of he vertex sending the message.
   * @param destination ID of the vertex to receive the message.
   * @param vertexState State of the sending vertex.
   * @param edgeWeight Weight of the edge joining the two vertices.
   * @return A map with one entry, sender -> messageToNeighbor
   */
  private def calculateMessage(sender: VertexId, destination: VertexId, vertexState: VertexState, edgeWeight: Double): Map[VertexId, Vector[Double]] = {

    val prior = vertexState.prior
    val messages = vertexState.messages

    val nStates = prior.length
    val stateRange = (0 to nStates - 1).toVector

    val messagesNotFromDestination = messages - destination
    val messagesNotFromDestinationValues: List[Vector[Double]] =
      messagesNotFromDestination.map({ case (k, v) => v }).toList

    val reducedMessages = VectorMath.overflowProtectedProduct(prior :: messagesNotFromDestinationValues).get

    val statesUNPosteriors = stateRange.zip(reducedMessages)

    val message = stateRange.map(i => statesUNPosteriors.map({
      case (j, x: Double) =>
        x * Math.exp(edgePotential(Math.abs(i - j) / (nStates - 1).toDouble, edgeWeight))
    }).reduce(_ + _))

    Map(sender -> message)
  }

  /**
   * Pregel required method to send messages across an edge.
   * @param edgeTriplet Contains state of source, destination and edge.
   * @return Iterator over messages to send.
   */
  private def sendMessage(edgeTriplet: EdgeTriplet[VertexState, Double]): Iterator[(VertexId, Map[Long, Vector[Double]])] = {

    val vertexState = edgeTriplet.srcAttr

    Iterator((edgeTriplet.dstId, calculateMessage(edgeTriplet.srcId, edgeTriplet.dstId, vertexState, edgeTriplet.attr)))
  }

  /**
   * Pregel required method to combine messages coming into a vertex.
   *
   * @param m1 First message.
   * @param m2 Second message.
   * @return Combined message.
   */
  private def mergeMsg(m1: Map[Long, Vector[Double]], m2: Map[Long, Vector[Double]]): Map[Long, Vector[Double]] = m1 ++ m2
}
