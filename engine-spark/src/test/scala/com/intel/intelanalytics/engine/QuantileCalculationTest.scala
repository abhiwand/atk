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

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.intelanalytics.algorithm.{ QuantileTarget, QuantileComposingElement }

class QuantileCalculationTest extends FlatSpec with Matchers {
  "25th quantile" should "be 0.5 * x2 + 0.5 * x3 from 10 elements" in {
    Seq(QuantileComposingElement(2, QuantileTarget(25, 0.5f)), QuantileComposingElement(3, QuantileTarget(25, 0.5f))) shouldBe SparkOps.getQuantileComposingElements(10, 25)
  }

  "0th quantile" should "be 1 * x1 + 0 * x2 from 5260980 elements" in {
    Seq(QuantileComposingElement(1, QuantileTarget(0, 1))) shouldBe SparkOps.getQuantileComposingElements(5260980, 0)
  }

  "95th quantile" should "be 1 * x4997931 + 0 * x4997932 from 5260980 elements" in {
    Seq(QuantileComposingElement(4997931, QuantileTarget(95, 1))) shouldBe SparkOps.getQuantileComposingElements(5260980, 95)
  }

  "99st quantile" should "be 1 * x4997931 + 0 * x4997932 from 5260980 elements" in {
    Seq(QuantileComposingElement(5208370, QuantileTarget(99, BigDecimal(0.8))), QuantileComposingElement(5208371, QuantileTarget(99, BigDecimal(0.2)))) shouldBe SparkOps.getQuantileComposingElements(5260980, 99)
  }

  "100th quantile" should "be 1 * x1 + 0 * x2 from 5260980 elements" in {
    Seq(QuantileComposingElement(5260980, QuantileTarget(100, 1))) shouldBe SparkOps.getQuantileComposingElements(5260980, 100)
  }

  "0th, 95th and 99st quantile" should "have mapping for element and mapping" in {
    val mapping = SparkOps.getQuantileTargetMapping(5260980, Seq(0, 95, 99))

    mapping(1) shouldBe Seq(QuantileTarget(0, 1))
    mapping(4997931) shouldBe Seq(QuantileTarget(95, 1))
    mapping(5208370) shouldBe Seq(QuantileTarget(99, 0.8))
    mapping(5208371) shouldBe Seq(QuantileTarget(99, 0.2))
  }

}
