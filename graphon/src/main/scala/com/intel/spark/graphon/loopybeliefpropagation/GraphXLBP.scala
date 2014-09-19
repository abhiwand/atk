package com.intel.spark.graphon.loopybeliefpropagation

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import com.intel.spark.graphon.iatpregel._
import com.intel.spark.graphon.iatpregel.IATPregelLogger
import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }

case class VertexState(gbVertex: GBVertex,
                       prior: List[Double],
                       unnormalizedPosterior: List[Double],
                       posterior: List[Double],
                       delta: Double)

object GraphXLBP {

  def runGraphXLBP(graph: Graph[VertexState, Double], maxIterations: Int, stateSpaceSize: Int): (Graph[VertexState, Double], String) = {

    // pregeling

    val initialMessage: List[Double] = (1 to stateSpaceSize).map(x => 1.toDouble).toList

    val power = 1.0d
    val smoothing = 1.0d

    def edgePotential(delta: Double, weight: Double) = {
      -Math.pow(delta.toDouble, power) * weight * smoothing
    }

    def vertexProgram(id: VertexId, vertexState: VertexState, incomingMessage: List[Double]): VertexState = {

      val prior = vertexState.prior

      val oldPosterior = vertexState.posterior

      val priorTimesMessages: List[Double] = VectorMath.product(prior, incomingMessage)

      // l1 normalization
      val l1Norm = priorTimesMessages.map(x => Math.abs(x)).reduce(_ + _)
      val normalizedPosterior = priorTimesMessages.map(x => (x / l1Norm))

      val delta = normalizedPosterior.zip(oldPosterior).map({ case (x, y) => Math.abs(x - y) }).reduce(_ + _)

      VertexState(vertexState.gbVertex, prior, priorTimesMessages, normalizedPosterior, delta)
    }

    def calculateMessage(unnormalizedPosterior: List[Double], edgeWeight: Double): List[Double] = {

      val nStates = unnormalizedPosterior.length

      val stateRange = (0 to nStates - 1).toList

      val statesUNPosteriors = stateRange.zip(unnormalizedPosterior)

      val message = stateRange.map(i => statesUNPosteriors.map({
        case (j, x: Double) =>
          x * Math.exp(edgePotential(Math.abs(i - j) / ((nStates - 1).toDouble), edgeWeight))
      }).reduce(_ + _))

      message
    }

    def sendMessage(edgeTriplet: EdgeTriplet[VertexState, Double]): Iterator[(VertexId, List[Double])] = {

      val unnormalizedPrior = edgeTriplet.srcAttr.unnormalizedPosterior

      Iterator((edgeTriplet.dstId, calculateMessage(unnormalizedPrior, edgeTriplet.attr)))
    }

    def mergeMsg(m1: List[Double], m2: List[Double]): List[Double] = VectorMath.product(m1, m2)

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

    val pregelLogger = IATPregelLogger(vertexDataToInitialStatus,
      InitialVertexCount.combine,
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
