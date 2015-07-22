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


package com.intel.taproot.analytics.engine.frame.plugins.bincolumn

import org.scalatest.WordSpec
import DiscretizationFunctions._

class DiscretizationFunctionsTest extends WordSpec {

  "DiscretizationFunctions.getBinEqualWidthCutoffs" should {

    "calculate correct equal width for 5 buckets" in {
      assert(getBinEqualWidthCutoffs(5, 0, 1) === Array(0.0, 0.2, 0.4, 0.6, 0.8, 1.0))
    }

    "calculate correct equal width for 9 buckets" in {
      assert(getBinEqualWidthCutoffs(9, 0, 90) === Array(0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0))
    }

    "calculate correct equal width for 10 buckets" in {
      assert(getBinEqualWidthCutoffs(10, 6.1245, 72.3453) === Array(6.1245, 12.74658, 19.36866, 25.99074, 32.61282, 39.234899999999996, 45.85698, 52.47906, 59.101139999999994, 65.72322, 72.3453))
    }

    "calculate correct number of equal width buckets even when the min and max have double imprecision" in {
      // This test was for a bug caused by double imprecision
      for {
        minAdjustment <- 1 to 20
        maxAdjustment <- 1 to 20
        numBins <- 1 to 10
      } {
        val min = 1 + (1/minAdjustment.toDouble)
        val max = 10 + (1/maxAdjustment.toDouble)
        val cutoffsArray = getBinEqualWidthCutoffs(numBins, min, max)
        //println(s"For numbBins:$numBins, Min:$min, Max:$max, Cutoffs array was:${cutoffsArray.toList}")
        assert(cutoffsArray.length === numBins + 1, s"For numbBins:$numBins, Min:$min, Max:$max, Cutoffs array was:${cutoffsArray.toList}")
      }

    }

  }
}
