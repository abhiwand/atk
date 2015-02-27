//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.spark.graphon.connectedcomponents

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

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
  def normalize(vertexToCCMap: RDD[(Long, Long)], sc: SparkContext): (Long, RDD[(Long, Long)]) = {

    // TODO implement this procedure properly when the number of connected components is enormous
    // it certainly makes sense to run this when the number of connected components requires a cluster to be stored
    // as an RDD of longs (say, if the input was many billions of disconnected nodes...)
    // BUT that may not be a sensible use case for a long time to come... and the code to handle that is significantly
    // more complicated.
    // FOR NOW... we  use the artificial restriction that there are at most 10 million connected components
    // if start tripping on that, we do this in a distributed fashion...

    val baseComponentRDD = vertexToCCMap.map(x => x._2).distinct()
    val count = baseComponentRDD.count()

    require(count < 10000000,
      "NormalizeConnectedComponents: Failed assumption: The number of connected components exceeds ten million."
        + "Please consider a fully distributed implementation of ID normalization. Thank you.")

    val componentArray = baseComponentRDD.toArray()
    val range = 1.toLong to count.toLong

    val zipped = componentArray.zip(range)
    val zippedAsRDD = sc.parallelize(zipped)

    val outRDD = vertexToCCMap.map(x => (x._2, x._1)).join(zippedAsRDD).map(x => x._2)
    (count, outRDD)
  }
}
