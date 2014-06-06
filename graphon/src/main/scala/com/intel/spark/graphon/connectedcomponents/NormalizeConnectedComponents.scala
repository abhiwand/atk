package com.intel.spark.graphon.connectedcomponents

import org.apache.spark.rdd.RDD

/**
 * Normalizes the vertex-to-connected components mapping so that community IDs come from a range
 * 1 to # of connected components (rather than simply having distinct longs as the IDs of the components).
 */
object NormalizeConnectedComponents {

  /**
   *
   * @param vertexToCCMap Map of each vertex ID to its component ID.
   * @return Pair consisting of number of connected components
   */
  def normalize (vertexToCCMap : RDD[(Long, Long)]) : (Long, RDD[(Long, Long)]) = {
    (0, vertexToCCMap)
  }
}
