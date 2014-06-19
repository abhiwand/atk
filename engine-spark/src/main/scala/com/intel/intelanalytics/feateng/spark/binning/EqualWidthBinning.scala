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

object EqualWidthBinning extends Binning[Double] {

  override def bin(inputRdd: RDD[Double], numBins: Int): Array[(Double, Int)] = {
    require(numBins >= 1, "Invalid number of bins: " + numBins)
    // TODO: save cutoffs and binSizes somewhere
    val (cutoffs: Array[Double], binSizes: Array[Long]) = inputRdd.histogram(numBins)

    cutoffs.foreach(println(_))
    binSizes.foreach(println(_))

    // map each data element to its bin id, using cutoffs index as bin id
    val binnedRdd = inputRdd.map { element ⇒
      var index = 0
      var working = true
      do {
        for (i ← 0 to cutoffs.length - 2) {
          // inclusive upper-bound on last cutoff range
          if ((i == cutoffs.length - 2) && (element - cutoffs(i) >= 0.0) && (element - cutoffs(i + 1) <= 0.0)) {
            index = i
            working = false
          } else if ((element - cutoffs(i) >= 0.0) && (element - cutoffs(i + 1) < 0.0)) {
            index = i
            working = false
          }
        }
      } while (working)
      (element, index)
    }
    binnedRdd.toArray()
  }

}
