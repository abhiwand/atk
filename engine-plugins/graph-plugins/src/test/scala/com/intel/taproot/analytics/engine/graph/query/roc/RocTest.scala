/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.analytics.engine.graph.query.roc

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
