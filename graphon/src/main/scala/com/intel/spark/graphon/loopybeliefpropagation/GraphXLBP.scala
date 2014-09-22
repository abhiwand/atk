package com.intel.spark.graphon.loopybeliefpropagation

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import com.intel.spark.graphon.iatpregel._
import com.intel.spark.graphon.iatpregel.IATPregelLogger
import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }

case class VertexState(gbVertex: GBVertex,
                       messages: Map[VertexId, Vector[Double]],
                       prior: Vector[Double],
                       posterior: Vector[Double],
                       delta: Double)

object GraphXLBP {

  def runGraphXLBP(graph: Graph[VertexState, Double], maxIterations: Int, stateSpaceSize: Int): (Graph[VertexState, Double], String) = {

    // pregeling

    val initialMessage: Map[Long, Vector[Double]] = Map()

    val power = 1.0d
    val smoothing = 1.0d

    def edgePotential(delta: Double, weight: Double) = {
      -Math.pow(delta.toDouble, power) * weight * smoothing
    }

    def vertexProgram(id: VertexId, vertexState: VertexState, messages: Map[VertexId, Vector[Double]]): VertexState = {

      val prior = vertexState.prior

      val oldPosterior = vertexState.posterior

      val priorTimesMessages: Vector[Double] =
        if (messages.nonEmpty) {
          VectorMath.product(prior, messages.values.reduce(VectorMath.product(_, _)))
        }
        else {
          prior
        }

      // l1 normalization
      val l1Norm = priorTimesMessages.map(x => Math.abs(x)).reduce(_ + _)
      val posterior = VectorMath.l1Normalize(priorTimesMessages)

      val delta = posterior.zip(oldPosterior).map({ case (x, y) => Math.abs(x - y) }).reduce(_ + _)

      VertexState(vertexState.gbVertex, messages, prior, posterior, delta)
    }

    def calculateMessage(sender: VertexId, destination: VertexId, vertexState: VertexState, edgeWeight: Double): Map[VertexId, Vector[Double]] = {

      val prior = vertexState.prior
      val messages = vertexState.messages

      val nStates = prior.length
      val stateRange = (0 to nStates - 1).toVector

      val messagesNotFromDestination = messages - destination

      val reducedMessages = if (messagesNotFromDestination.nonEmpty) {
        VectorMath.product(prior, messagesNotFromDestination.values.reduce(VectorMath.product(_, _)))
      }
      else {
        prior
      }

      val statesUNPosteriors = stateRange.zip(reducedMessages)

      val message = stateRange.map(i => statesUNPosteriors.map({
        case (j, x: Double) =>
          x * Math.exp(edgePotential(Math.abs(i - j) / ((nStates - 1).toDouble), edgeWeight))
      }).reduce(_ + _))

      Map(sender -> message)
    }

    def sendMessage(edgeTriplet: EdgeTriplet[VertexState, Double]): Iterator[(VertexId, Map[Long, Vector[Double]])] = {

      val vertexState = edgeTriplet.srcAttr

      Iterator((edgeTriplet.dstId, calculateMessage(edgeTriplet.srcId, edgeTriplet.dstId, vertexState, edgeTriplet.attr)))
    }

    def mergeMsg(m1: Map[Long, Vector[Double]], m2: Map[Long, Vector[Double]]): Map[Long, Vector[Double]] = m1 ++ m2

    // logging

    def vertexDataToInitialStatus(vdata: VertexState) = InitialVertexCount(1)
    def edgeDataToInitialStatus(edata: Double) = InitialEdgeCount(1)
    def convertStateToStatus(state: VertexState): SuperStepCountNetDelta = SuperStepCountNetDelta(1, state.delta)

    def generateInitialReport(initialVertexStatus: InitialVertexCount, initialEdgeStatus: InitialEdgeCount) = {
      var report = new StringBuilder("**** LOOPY BELIEF PROPAGATION ****\n")

      report.++=("vertex count = " + initialVertexStatus.vertexCount + "\n")
      report.++=("edge count = " + initialEdgeStatus.edgeCount + "\n")
      report.++=("max number of supersteps = " + maxIterations + "\n")

      report.toString()
    }

    val pregelLogger = IATPregelLogger(InitialVertexCount.emptyInitialStatus,
      vertexDataToInitialStatus,
      InitialVertexCount.combine,
      InitialEdgeCount.emptyInitalStatus,
      edgeDataToInitialStatus,
      InitialEdgeCount.combine,
      generateInitialReport,
      SuperStepCountNetDelta.accumulateSuperStepStatus,
      convertStateToStatus,
      SuperStepCountNetDelta.generateStepReport)

    IATPregel(graph,
      initialMessage,
      pregelLogger,
      maxIterations = maxIterations,
      activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, mergeMsg)
  }
}
