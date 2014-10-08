package com.intel.spark.graphon.iatpregel

import org.apache.spark.rdd.RDD

/**
 * Provides a method for creating an initial report that summarizes vertex and edge counts.
 *
 * @tparam V Class of the vertex data in the graph.
 * @tparam E Class of the edge data in the graph.
 */
class BasicCountsInitialReport[V, E] extends InitialReport[V, E] with Serializable {

  private def vertexToCount(v: V): Long = 1
  private def edgeToCount(e: E): Long = 1

  /**
   * @param vertices RDD of the per-vertex data.
   * @param edges RDD of the per-edge data.
   * @return Initial report that summarizes vertex and edge counts.
   */
  def generateInitialReport(vertices: RDD[V], edges: RDD[E]): String = {

    val sc = vertices.context
    require(sc == edges.context, "Graph has its edge and vertex RDDs in distinct spark contexts.")

    val dummyCountRDD: RDD[Long] = sc.parallelize(List(0.toLong)) // Spark should provide a "reduceWithIdentity"

    val vertexCount = vertices.map(v => vertexToCount(v)).union(dummyCountRDD).reduce(_ + _)
    val edgeCount = edges.map(e => edgeToCount(e)).union(dummyCountRDD).reduce(_ + _)

    "Vertex Count: " + vertexCount + "\nEdge Count: " + edgeCount + "\n"
  }

}

/**
 * Aggregater for per-superstep status reports.
 *
 * @param vertexCount Number of vertices.
 * @param sumOfDeltas Net change.
 */
case class SuperStepCountNetDelta(vertexCount: Long, sumOfDeltas: Double) extends Serializable

/**
 * Provides a method for generating per-superstep reports that summarizes the vertex count and average change per
 * vertex since the last superstep.
 *
 * @tparam V Class of the vertex data.
 */

class AverageDeltaSuperStepReport[V <: DeltaProvider] extends SuperStepReport[V] with Serializable {

  private def accumulateSuperStepStatus(status1: SuperStepCountNetDelta, status2: SuperStepCountNetDelta) = {
    new SuperStepCountNetDelta(status1.vertexCount + status2.vertexCount, status1.sumOfDeltas + status2.sumOfDeltas)
  }

  private def convertVertexDataToStatus(v: V) = SuperStepCountNetDelta(vertexCount = 1, sumOfDeltas = v.delta)

  /**
   *
   * @param iteration Number of the Pregel superstep that has just completed.
   * @param vertices RDD of the per-vertex data.
   * @return Summary of the vertex count and average change per vertex since the last superstep.
   */
  def generateSuperStepReport(iteration: Int, vertices: RDD[V]) = {

    val status = vertices.map(v => convertVertexDataToStatus(v)).reduce(accumulateSuperStepStatus)

    "IATPregel engine has completed iteration " + iteration + "  " +
      "The average delta is " + (status.sumOfDeltas / status.vertexCount + "\n")
  }

}

