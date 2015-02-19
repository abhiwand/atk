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

package com.intel.intelanalytics.engine.spark.graph.query.roc

import org.scalatest.WordSpec

class RocTest extends WordSpec {

  "RocCounts" should {
    "have a single arg constructor" in {
      val rocCounts = new RocCounts(99)
      assert(rocCounts.numFalsePositives.length == 99)
      assert(rocCounts.numTruePositives.length == 99)
      assert(rocCounts.numNegatives.length == 99)
      assert(rocCounts.numPositives.length == 99)
    }

    "be mergable" in {
      val rocCounts1 = new RocCounts(99)
      val rocCounts2 = new RocCounts(99)

      rocCounts1.numFalsePositives.update(0, 1)
      rocCounts2.numFalsePositives.update(0, 2)

      rocCounts1.numFalsePositives.update(1, 11)
      rocCounts2.numFalsePositives.update(1, 99)

      val result = rocCounts1.merge(rocCounts2)

      assert(result.numFalsePositives(0) == 3)
      assert(result.numFalsePositives(1) == 110)
    }
  }

  "RocParams" should {
    "calculate correct size" in {
      val rocParams = RocParams(List(3, 1, 6))
      assert(rocParams.size == 3)
    }

    "calculate correct thresholds" in {
      val rocParams = RocParams(List(3, 1, 6))
      assert(rocParams.thresholds == Vector(3, 4, 5))
    }
  }
}
