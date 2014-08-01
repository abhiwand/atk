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

import org.scalatest.Matchers
import com.intel.testutils.TestingSparkContextFlatSpec
import com.intel.intelanalytics.domain.schema.DataTypes
import scala.collection.mutable.ListBuffer
import com.intel.intelanalytics.algorithm.Quantile

class QuantileCalculationITest extends TestingSparkContextFlatSpec with Matchers {
  "Calculation quantile in small data set" should "return the correct values" in {
    val numbers = List((Array[Any](3, "")), (Array[Any](5, "")),
      (Array[Any](6, "")), (Array[Any](7, "")), (Array[Any](23, "")), (Array[Any](8, "")), (Array[Any](21, "")), (Array[Any](9, "")), (Array[Any](11, "")),
      (Array[Any](20, "")), (Array[Any](13, "")), (Array[Any](15, "")), (Array[Any](10, "")), (Array[Any](16, "")), (Array[Any](17, "")),
      (Array[Any](18, "")), (Array[Any](1, "")), (Array[Any](19, "")), (Array[Any](4, "")), (Array[Any](22, "")),
      (Array[Any](24, "")), (Array[Any](12, "")), (Array[Any](2, "")), (Array[Any](14, "")), (Array[Any](25, ""))
    )

    val rdd = sparkContext.parallelize(numbers, 3)
    val result = SparkOps.calculateQuantiles(rdd, Seq(0, 3, 5, 40, 40.5, 100), 0, DataTypes.int32)
    result.length shouldBe 6
    result(0) shouldBe Quantile(0, 1)
    result(1) shouldBe Quantile(3, 1)
    result(2) shouldBe Quantile(5, 1.25)
    result(3) shouldBe Quantile(40, 10)
    result(4) shouldBe Quantile(40.5, 10.125)
    result(5) shouldBe Quantile(100, 25)
  }

  //   Large scale test takes longer time. uncomment it when needed.
  //  "Calculation percentile in large data set" should "return the correct values" in {
  //
  //    import scala.util.Random
  //    val numbers = ListBuffer[Array[Any]]()
  //    numbers
  //    for (i <- 1 to 1000000) {
  //      numbers += Array[Any](i, "")
  //    }
  //
  //    val randomPositionedNumbers = Random.shuffle(numbers)
  //
  //    val rdd = sc.parallelize(randomPositionedNumbers, 90)
  //    val result = SparkOps.calculatePercentiles(rdd, Seq(5, 40), 0, DataTypes.int32)
  //    result.length shouldBe 2
  //    result(0) shouldBe(5, 50000)
  //    result(1) shouldBe(40, 400000)
  //  }
}
