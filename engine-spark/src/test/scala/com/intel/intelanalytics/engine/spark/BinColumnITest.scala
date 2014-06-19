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

package com.intel.intelanalytics.engine.spark

import com.intel.intelanalytics.engine.testutils.TestingSparkContext
import org.specs2.mutable.Specification

class BinColumnITest extends Specification {

  "binEqualWidth" should {

    "create append new column" in new TestingSparkContext {
      val inputList = List(Array[Any]("A", 1), Array[Any]("B", 2), Array[Any]("C", 3), Array[Any]("D", 4), Array[Any]("E", 5))
      val rdd = sc.parallelize(inputList)
      val binnedRdd = SparkOps.binEqualWidth(1, 2, rdd)
      val result = binnedRdd.take(5)
      result.apply(0) mustEqual Array[Any]("A", 1, 0)
      result.apply(1) mustEqual Array[Any]("B", 2, 0)
      result.apply(2) mustEqual Array[Any]("C", 3, 1)
      result.apply(3) mustEqual Array[Any]("D", 4, 1)
      result.apply(4) mustEqual Array[Any]("E", 5, 1)
    }
  }

}
