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

package com.intel.intelanalytics.engine.spark.frame.plugins.cumulativedist

import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers
import com.intel.intelanalytics.domain.schema.{ DataTypes, Column }

class ECDFTest extends TestingSparkContextFlatSpec with Matchers {

  // Input data
  val sampleOneList = List(
    Array[Any](0),
    Array[Any](1),
    Array[Any](2),
    Array[Any](3),
    Array[Any](4),
    Array[Any](5),
    Array[Any](6),
    Array[Any](7),
    Array[Any](8),
    Array[Any](9))

  val sampleTwoList = List(
    Array[Any](0),
    Array[Any](0),
    Array[Any](0),
    Array[Any](0),
    Array[Any](4),
    Array[Any](5),
    Array[Any](6),
    Array[Any](7))

  val sampleThreeList = List(
    Array[Any](-2),
    Array[Any](-1),
    Array[Any](0),
    Array[Any](1),
    Array[Any](2))

  "ecdf" should "compute correct ecdf" in {

    val sampleOneRdd = sparkContext.parallelize(sampleOneList, 2)
    val sampleTwoRdd = sparkContext.parallelize(sampleTwoList, 2)
    val sampleThreeRdd = sparkContext.parallelize(sampleThreeList, 2)

    // Get binned results
    val sampleOneECDF = CumulativeDistFunctions.ecdf(sampleOneRdd, Column("a", DataTypes.int32, 0))
    val resultOne = sampleOneECDF.take(10)

    val sampleTwoECDF = CumulativeDistFunctions.ecdf(sampleTwoRdd, Column("a", DataTypes.int32, 0))
    val resultTwo = sampleTwoECDF.take(5)

    val sampleThreeECDF = CumulativeDistFunctions.ecdf(sampleThreeRdd, Column("a", DataTypes.int32, 0))
    val resultThree = sampleThreeECDF.take(5)

    // Validate
    resultOne.apply(0) shouldBe Array[Any](0, 0.1)
    resultOne.apply(1) shouldBe Array[Any](1, 0.2)
    resultOne.apply(2) shouldBe Array[Any](2, 0.3)
    resultOne.apply(3) shouldBe Array[Any](3, 0.4)
    resultOne.apply(4) shouldBe Array[Any](4, 0.5)
    resultOne.apply(5) shouldBe Array[Any](5, 0.6)
    resultOne.apply(6) shouldBe Array[Any](6, 0.7)
    resultOne.apply(7) shouldBe Array[Any](7, 0.8)
    resultOne.apply(8) shouldBe Array[Any](8, 0.9)
    resultOne.apply(9) shouldBe Array[Any](9, 1.0)

    resultTwo.apply(0) shouldBe Array[Any](0, 0.5)
    resultTwo.apply(1) shouldBe Array[Any](4, 0.625)
    resultTwo.apply(2) shouldBe Array[Any](5, 0.75)
    resultTwo.apply(3) shouldBe Array[Any](6, 0.875)
    resultTwo.apply(4) shouldBe Array[Any](7, 1.0)

    resultThree.apply(0) shouldBe Array[Any](-2, 0.2)
    resultThree.apply(1) shouldBe Array[Any](-1, 0.4)
    resultThree.apply(2) shouldBe Array[Any](0, 0.6)
    resultThree.apply(3) shouldBe Array[Any](1, 0.8)
    resultThree.apply(4) shouldBe Array[Any](2, 1.0)
  }

}
