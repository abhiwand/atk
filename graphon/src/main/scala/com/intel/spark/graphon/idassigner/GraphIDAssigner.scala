package com.intel.spark.graphon.idassigner

import org.apache.spark.rdd.RDD

/**
 * Work in progress. Assign unique vertex IDs to graph elements.
 */
class GraphIDAssigner {


  def getVertexIDs(vertices : RDD[Any]) =  ???
    // TBD
/*
    val numberOfPartitions = vertices.partitions.length
    val partitionRange = 0 to (numberOfPartitions - 1)

    val partitionSizes = vertices.glom().map(array => array.length).toArray()

    val offsets = partitionRange.map(i => partitionSizes.scanLeft(0)(_ + _).take(numberOfPartitions))

    def idAssigner(index: Int, value : Any) = {
*/

}
