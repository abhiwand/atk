package com.intel.spark.graphon.loopybeliefpropagation

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import com.intel.spark.graphon.IATPregel

case class VertexState(values: Array[Double], id: Any)

object GraphXLBP {

  def runGraphXLBP(graph: Graph[VertexState, Double]): (Graph[VertexState, Double], String) = {

    val initialMessage: Double = 0

    def vertexProgram(id: VertexId, vertexState: VertexState, edgeState: Double): VertexState = {
      vertexState
    }

    def sendMessage(edgeTriplet: EdgeTriplet[VertexState, Double]): Iterator[(VertexId, Double)] = {
      Iterator()
    }

    def mergeMsg(message1: Double, message2: Double): Double = {
      0
    }

    IATPregel(graph, initialMessage, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, mergeMsg)
  }
}
