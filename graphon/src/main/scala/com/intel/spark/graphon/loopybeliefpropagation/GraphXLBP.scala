package com.intel.spark.graphon.loopybeliefpropagation

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import com.intel.spark.graphon.IATPregel

case class VertexState(value: Double, id: Any, delta: Double)

case class InitialVertexStatus(vertexCount: Long)

case class InitialEdgeStatus(edgeCount: Long)

case class SuperStepStatus(vertexCount: Long, sumOfDeltas: Double)

object GraphXLBP {

  def runGraphXLBP(graph: Graph[VertexState, Double], maxIterations: Int): (Graph[VertexState, Double], String) = {

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

    def accumulateStatus(status1: SuperStepStatus, status2: SuperStepStatus) = {

      SuperStepStatus(status1.vertexCount + status2.vertexCount, status1.sumOfDeltas + status2.sumOfDeltas)
    }

    def convertStateToStatus(state: VertexState): SuperStepStatus = SuperStepStatus(1, state.delta)

    def generateStepReport(status: SuperStepStatus, iteration: Int) = {
      "IATPregel has completed iteration " + iteration + ".  \n FEAR IATPREGEL !!!!" +
        "The average delta is " + (status.sumOfDeltas / status.vertexCount)
    }

    def vertexDataToInitialStatus(vdata: VertexState) = InitialVertexStatus(1)
    def vertexInitialStatusCombiner(status1: InitialVertexStatus, status2: InitialVertexStatus) =
      InitialVertexStatus(status1.vertexCount + status2.vertexCount)

    def edgeDataToInitialStatus(edata: Double) = InitialEdgeStatus(1)
    def edgeInitialStatusCombiner(status1: InitialEdgeStatus, status2: InitialEdgeStatus) =
      InitialEdgeStatus(status1.edgeCount + status2.edgeCount)

    def generateInitialReport(initialVertexStatus: InitialVertexStatus, initialEdgeStatus: InitialEdgeStatus) = {
      var report = new StringBuilder("**** LOOPY BELIEF PROPAGATION ****\n")

      report.++=("vertex count = " + initialVertexStatus.vertexCount + "\n")
      report.++=("edge count = " + initialEdgeStatus.edgeCount + "\n")
      report.++=("max number of supersteps = " + maxIterations + "\n")

      report.toString()
    }

    IATPregel(graph,
      initialMessage,
      vertexDataToInitialStatus,
      vertexInitialStatusCombiner,
      edgeDataToInitialStatus,
      edgeInitialStatusCombiner,
      generateInitialReport,
      accumulateStatus,
      convertStateToStatus,
      generateStepReport,
      maxIterations = maxIterations,
      activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, mergeMsg)
  }
}
