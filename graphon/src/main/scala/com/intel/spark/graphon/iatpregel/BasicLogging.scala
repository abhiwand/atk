package com.intel.spark.graphon.iatpregel

case class InitialVertexCount(vertexCount: Long)

object InitialVertexCount {
  def combine(status1: InitialVertexCount, status2: InitialVertexCount) =
    InitialVertexCount(status1.vertexCount + status2.vertexCount)

  def emptyInitialStatus : InitialVertexCount = {
    InitialVertexCount(0)
  }
}

case class InitialEdgeCount(edgeCount: Long)

object InitialEdgeCount {
  def combine(status1: InitialEdgeCount, status2: InitialEdgeCount) =
    InitialEdgeCount(status1.edgeCount + status2.edgeCount)

  def emptyInitalStatus : InitialEdgeCount = {
    InitialEdgeCount(0)
  }
}

case class SuperStepCountNetDelta(vertexCount: Long, sumOfDeltas: Double)

object SuperStepCountNetDelta {

  def accumulateSuperStepStatus(status1: SuperStepCountNetDelta, status2: SuperStepCountNetDelta) = {
    new SuperStepCountNetDelta(status1.vertexCount + status2.vertexCount, status1.sumOfDeltas + status2.sumOfDeltas)
  }

  def generateStepReport(status: SuperStepCountNetDelta, iteration: Int) = {
    "IATPregel engine has completed iteration " + iteration + "  " +
      "The average delta is " + (status.sumOfDeltas / status.vertexCount + "\n")
  }
}
