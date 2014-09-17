package com.intel.spark.graphon.loopybeliefpropagation

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import com.intel.spark.graphon.iatpregel._
import com.intel.spark.graphon.iatpregel.IATPregelLogger

case class VertexState(value: Double, id: Any, delta: Double)

object GraphXLBP {

  def runGraphXLBP(graph: Graph[VertexState, Double], maxIterations: Int): (Graph[VertexState, Double], String) = {

    // pregeling

    val initialMessage: Double = 0

    def vertexProgram(id: VertexId, vertexState: VertexState, message: Double): VertexState = {

      val oldValue = vertexState.value
      val newValue = oldValue - message
      val delta = message

      VertexState(newValue, vertexState.id, delta)
    }

    def sendMessage(edgeTriplet: EdgeTriplet[VertexState, Double]): Iterator[(VertexId, Double)] = {
      Iterator((edgeTriplet.dstId, edgeTriplet.srcAttr.value))
    }

    def mergeMsg(message1: Double, message2: Double): Double = {
      0.05d
    }

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
