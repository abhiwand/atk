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
  def normalize (vertexToCCMap : RDD[(Long, Long)], sc: SparkContext) : (Long, RDD[(Long, Long)]) = {

    // TODO implement this procedure properly when the number of connected components is enormous
    // it certainly makes sense to run this when the number of connected components requires a cluster to be stored
    // as an RDD of longs,  and the but it may not be a sensible use case for a long time to come...
    // FOR NOW... articifical restriction that there are at most 10 million connected components

    val baseComponentRDD = vertexToCCMap.map(x => x._2).distinct()
    val count = baseComponentRDD.count()

    require(count < 10000000)

    val componentArray = baseComponentRDD.toArray()
    val range = 1.toLong to count.toLong

    val zipped = componentArray.zip(range)
    val zippedAsRDD = sc.parallelize(zipped)

    val outRDD = vertexToCCMap.map(x => (x._2, x._1)).join(zippedAsRDD).map(x => x._2)
    (count, outRDD)
  }
}
