package com.intel.spark.graphon.iatpregel

import org.apache.spark.rdd.RDD

/**
 * Implementations of this trait provide a method for generating a summary of superstep activity afte the completion
 * of a Pregel superstep.
 * @tparam V Class of the vertex data.
 */

trait SuperStepReport[V] extends Serializable {
  def generateSuperStepReport(iteration: Int, vertices: RDD[V]): String
}
