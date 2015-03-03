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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.{ BeforeAndAfterEach, FlatSpec, Matchers }

class FlattenColumnArgsITest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContextFlatSpec {
  "flattenRddByStringColumnIndex" should "create separate rows when flattening entries" in {
    val carOwnerShips = List(Array[Any]("Bob", "Mustang,Camry"), Array[Any]("Josh", "Neon,CLK"), Array[Any]("Alice", "PT Cruiser,Avalon,F-150"), Array[Any]("Tim", "Beatle"), Array[Any]("Becky", ""))
    val rdd = sparkContext.parallelize(carOwnerShips)
    val flattened = FlattenColumnFunctions.flattenRddByStringColumnIndex(",")(1, rdd)
    val result = flattened.take(9)
    result.apply(0) shouldBe Array[Any]("Bob", "Mustang")
    result.apply(1) shouldBe Array[Any]("Bob", "Camry")
    result.apply(2) shouldBe Array[Any]("Josh", "Neon")
    result.apply(3) shouldBe Array[Any]("Josh", "CLK")
    result.apply(4) shouldBe Array[Any]("Alice", "PT Cruiser")
    result.apply(5) shouldBe Array[Any]("Alice", "Avalon")
    result.apply(6) shouldBe Array[Any]("Alice", "F-150")
    result.apply(7) shouldBe Array[Any]("Tim", "Beatle")
    result.apply(8) shouldBe Array[Any]("Becky", "")

  }
}

class FlattenVectorColumnArgsITest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContextFlatSpec {
  "flattenRddByVectorColumnIndex" should "create separate rows when flattening entries" in {
    val vectorShips = List(Array[Any]("counting", Vector[Double](1.2, 3.4, 5.6)),
      Array[Any]("pi", Vector[Double](3.14)),
      Array[Any]("neg", Vector[Double](-1.0, -5.5)))
    val rdd = sparkContext.parallelize(vectorShips)
    val flattened = FlattenColumnFunctions.flattenRddByVectorColumnIndex(1, rdd)
    val result = flattened.take(6)
    result.apply(0) shouldBe Array[Any]("counting", 1.2)
    result.apply(1) shouldBe Array[Any]("counting", 3.4)
    result.apply(2) shouldBe Array[Any]("counting", 5.6)
    result.apply(3) shouldBe Array[Any]("pi", 3.14)
    result.apply(4) shouldBe Array[Any]("neg", -1.0)
    result.apply(5) shouldBe Array[Any]("neg", -5.5)
  }
}
