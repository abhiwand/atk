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
}
