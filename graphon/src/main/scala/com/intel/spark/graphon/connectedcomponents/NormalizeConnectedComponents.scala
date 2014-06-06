package com.intel.spark.graphon.connectedcomponents

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.rdd._

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

    val count = vertexToCCMap.map(x=> x._2).distinct().count()

    vertexToCCMap.sortByKey()
    vertexToCCMap.map(x=> x._2).distinct()
    (count, vertexToCCMap)
  }
}
