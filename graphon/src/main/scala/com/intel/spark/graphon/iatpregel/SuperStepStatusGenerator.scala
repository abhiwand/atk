package com.intel.spark.graphon.iatpregel

import org.apache.spark.rdd.RDD

case class SuperStepStatus(log: String, earlyTermination: Boolean)

/**
 * Implementations of this trait provide a method for generating a summary of superstep activity after the completion
 * of a Pregel superstep.
 * @tparam V Class of the vertex data.
 */

trait SuperStepStatusGenerator[V] extends Serializable {
  def generateSuperStepStatus(iteration: Int, totalVertices: Long, activeVertices: RDD[V]): SuperStepStatus
}

