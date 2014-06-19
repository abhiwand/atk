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

import org.specs2.mutable.Specification
import com.intel.intelanalytics.feateng.spark.testutils.TestingSparkContext

class EqualWidthBinningSpec extends Specification {

  "Equal width binning" should {

    "create the correct number of bins" in new TestingSparkContext {
      // Input data
      val inputSeq = Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)
      val inputRdd = sc.parallelize(inputSeq)

      // Compute equal width bins
      val binnedData = EqualWidthBinning.bin(inputRdd, 5)

      // Validate
      binnedData.map(binMapping ⇒ binMapping._2).distinct.size mustEqual 5
    }

    "create equal width bins" in new TestingSparkContext {
      // Input data
      val inputSeq = Seq(1.0, 1.2, 1.4, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 10.0)
      val inputRdd = sc.parallelize(inputSeq)

      // Compute equal width bins
      val binnedData = EqualWidthBinning.bin(inputRdd, 5)

      // Validate
      // TODO: need access to cutoffs in order to test bin widths
      true mustEqual false
    }

    "bin all of the data" in new TestingSparkContext {
      // Input data
      val inputSeq = Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)
      val inputRdd = sc.parallelize(inputSeq)

      // Compute equal width bins
      val binnedData = EqualWidthBinning.bin(inputRdd, 5)

      // Validate
      binnedData.map(binMapping ⇒ binMapping._1) mustEqual inputSeq.toArray
    }

    "not split elements with same value across bins" in new TestingSparkContext {
      // Input data
      val inputSeq = Seq(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 9.0, 10.0)
      val inputRdd = sc.parallelize(inputSeq)

      // Compute equal width bins
      val binnedData = EqualWidthBinning.bin(inputRdd, 5)

      // Validate
      binnedData.map(binMapping ⇒ binMapping._2) mustEqual Array(0, 0, 0, 0, 0, 0, 0, 0, 4, 4)
    }

  }

}
