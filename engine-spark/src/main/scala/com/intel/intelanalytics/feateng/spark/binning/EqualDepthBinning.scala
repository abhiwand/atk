//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.feateng.spark.binning

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.math.ceil

object EqualDepthBinning extends Binning[Double] {

  /**
   * Bin data into equal depth bins.  The numBins parameter is an upper-bound on the number of bins since the data may
   * justify fewer bins.  For example, if the inputRdd has 10 elements with only 2 distinct values and numBins > 2,
   * then the number of actual bins will only be 2.  This is due to a restriction that elements with the same value
   * must belong to the same bin.
   *
   * @param inputRdd the input data, representing a column of feature values
   * @param numBins an upper-bound on the desired number of bins
   * @return an array of tuples that map each element to a bin number
   */
  override def bin(inputRdd: RDD[Double], numBins: Int): Array[(Double, Int)] = {
    require(numBins >= 1, "Invalid number of bins: " + numBins)

    val numElements = inputRdd.count()

    // assign a rank to each distinct element
    val pairedRdd = inputRdd.groupBy(element ⇒ element).sortByKey()
    var rank = 1
    val rankedRdd = pairedRdd.map { value ⇒
      val valueRank = (rank to (rank + (value._2.size - 1))).sum / value._2.size.asInstanceOf[Double]
      val valuePairs = value._2.map(v ⇒ (v, valueRank))
      rank += value._2.size
      (value._1, valuePairs)
    }.flatMap(mapping ⇒ mapping._2)

    // compute the bin number
    val binnedRdd = rankedRdd.map { ranking ⇒
      // not sure why scala.math.ceil returns Double instead of Int or Long
      val bin = ceil((numBins * ranking._2) / numElements).asInstanceOf[Int]
      (ranking._1, bin)
    }

    // shift the bin numbers so that they are zero-based contiguous values
    val sortedBinnedRdd = binnedRdd.groupBy(_._2).sortByKey()
    rank = 1
    val shiftedRdd = sortedBinnedRdd.map { value ⇒
      val valuePairs = value._2.map(v ⇒ (v._1, rank))
      rank += 1
      (value._1, valuePairs)
    }.flatMap(mapping ⇒ mapping._2)

    shiftedRdd.toArray()
  }

}
