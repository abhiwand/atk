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

import com.intel.intelanalytics.engine.TestingSparkContext
import org.apache.spark.SparkException
import org.scalatest.Matchers

class BinColumnITest extends TestingSparkContext with Matchers {

  "binEqualWidth" should "append new column" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5))
    val rdd = sc.parallelize(inputList)

    // Get binned results
    val binnedRdd = SparkOps.binEqualWidth(1, 2, rdd)
    val result = binnedRdd.take(5)

    // Validate
    result.apply(0) shouldBe Array[Any]("A", 1, 0)
    result.apply(1) shouldBe Array[Any]("B", 2, 0)
    result.apply(2) shouldBe Array[Any]("C", 3, 1)
    result.apply(3) shouldBe Array[Any]("D", 4, 1)
    result.apply(4) shouldBe Array[Any]("E", 5, 1)
  }

  "binEqualWidth" should "create the correct number of bins" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5))
    val rdd = sc.parallelize(inputList)

    // Get binned results
    val binnedRdd = SparkOps.binEqualWidth(1, 2, rdd)

    // Validate
    binnedRdd.map(row => row(2)).distinct.count() shouldEqual 2
  }

  "binEqualWidth" should "create equal width bins" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 1.5),
      Array[Any]("C", 2),
      Array[Any]("D", 3),
      Array[Any]("E", 4),
      Array[Any]("F", 4.5))
    val rdd = sc.parallelize(inputList, 2)

    // Get binned results
    val binnedRdd = SparkOps.binEqualWidth(1, 4, rdd)
    val result = binnedRdd.take(6)

    // Validate
    result.apply(0) shouldBe Array[Any]("A", 1, 0)
    result.apply(1) shouldBe Array[Any]("B", 1.5, 0)
    result.apply(2) shouldBe Array[Any]("C", 2, 1)
    result.apply(3) shouldBe Array[Any]("D", 3, 2)
    result.apply(4) shouldBe Array[Any]("E", 4, 3)
    result.apply(5) shouldBe Array[Any]("F", 4.5, 3)
  }

  "binEqualWidth" should "throw error if less than one bin requested" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 1.5),
      Array[Any]("C", 2),
      Array[Any]("D", 3),
      Array[Any]("E", 4),
      Array[Any]("F", 4.5))
    val rdd = sc.parallelize(inputList)

    // Get binned results
    an[IllegalArgumentException] shouldBe thrownBy(SparkOps.binEqualWidth(1, 0, rdd))
  }

  "binEqualWidth" should "throw error if attempting to bin non-numeric column" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 1.5),
      Array[Any]("C", 2),
      Array[Any]("D", 3),
      Array[Any]("E", 4),
      Array[Any]("F", 4.5))
    val rdd = sc.parallelize(inputList)

    // Get binned results
    a[SparkException] shouldBe thrownBy(SparkOps.binEqualWidth(0, 4, rdd))
  }

  "binEqualWidth" should "put each element in separate bin if num_bins is greater than length of column" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5),
      Array[Any]("F", 6),
      Array[Any]("G", 7),
      Array[Any]("H", 8),
      Array[Any]("I", 9),
      Array[Any]("J", 10))
    val rdd = sc.parallelize(inputList, 2)

    // Get binned results
    val binnedRdd = SparkOps.binEqualWidth(1, 20, rdd) // note this creates bins of width 0.55 for this dataset
    val result = binnedRdd.take(10)

    // Validate
    result.apply(0) shouldBe Array[Any]("A", 1, 0)
    result.apply(1) shouldBe Array[Any]("B", 2, 2)
    result.apply(2) shouldBe Array[Any]("C", 3, 4)
    result.apply(3) shouldBe Array[Any]("D", 4, 6)
    result.apply(4) shouldBe Array[Any]("E", 5, 8)
    result.apply(5) shouldBe Array[Any]("F", 6, 11)
    result.apply(6) shouldBe Array[Any]("G", 7, 13)
    result.apply(7) shouldBe Array[Any]("H", 8, 15)
    result.apply(8) shouldBe Array[Any]("I", 9, 17)
    result.apply(9) shouldBe Array[Any]("J", 10, 19)
  }

  "binEqualDepth" should "append new column" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5))
    val rdd = sc.parallelize(inputList, 2)

    // Get binned results
    val binnedRdd = SparkOps.binEqualDepth(1, 2, rdd)
    val result = binnedRdd.take(5)

    // Validate
    result.apply(0) shouldBe Array[Any]("A", 1, 0)
    result.apply(1) shouldBe Array[Any]("B", 2, 0)
    result.apply(2) shouldBe Array[Any]("C", 3, 1)
    result.apply(3) shouldBe Array[Any]("D", 4, 1)
    result.apply(4) shouldBe Array[Any]("E", 5, 1)
  }

  "binEqualDepth" should "create the correct number of bins" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5))
    val rdd = sc.parallelize(inputList, 2)

    // Get binned results
    val binnedRdd = SparkOps.binEqualDepth(1, 2, rdd)

    // Validate
    binnedRdd.map(row => row(2)).distinct.count() shouldEqual 2
  }

  "binEqualDepth" should "bin identical values in same bin, even if it means creating fewer than requested bins" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 1),
      Array[Any]("C", 1),
      Array[Any]("D", 1),
      Array[Any]("E", 5))
    val rdd = sc.parallelize(inputList, 2)

    // Get binned results
    val binnedRdd = SparkOps.binEqualDepth(1, 3, rdd)
    val result = binnedRdd.take(5)

    // Validate
    result.apply(0) shouldBe Array[Any]("A", 1, 0)
    result.apply(1) shouldBe Array[Any]("B", 1, 0)
    result.apply(2) shouldBe Array[Any]("C", 1, 0)
    result.apply(3) shouldBe Array[Any]("D", 1, 0)
    result.apply(4) shouldBe Array[Any]("E", 5, 1)
  }

  "binEqualDepth" should "create equal depth bins" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 1.2),
      Array[Any]("C", 1.5),
      Array[Any]("D", 1.6),
      Array[Any]("E", 3),
      Array[Any]("F", 6))
    val rdd = sc.parallelize(inputList, 2)

    // Get binned results
    val binnedRdd = SparkOps.binEqualDepth(1, 3, rdd)
    val result = binnedRdd.take(6)

    // Validate
    result.apply(0) shouldBe Array[Any]("A", 1, 0)
    result.apply(1) shouldBe Array[Any]("B", 1.2, 0)
    result.apply(2) shouldBe Array[Any]("C", 1.5, 1)
    result.apply(3) shouldBe Array[Any]("D", 1.6, 1)
    result.apply(4) shouldBe Array[Any]("E", 3, 2)
    result.apply(5) shouldBe Array[Any]("F", 6, 2)
  }

  "binEqualDepth" should "create equal depth bins - another test" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5),
      Array[Any]("F", 6),
      Array[Any]("G", 7),
      Array[Any]("H", 8),
      Array[Any]("I", 9),
      Array[Any]("J", 10))
    val rdd = sc.parallelize(inputList, 2)

    // Get binned results
    val binnedRdd = SparkOps.binEqualDepth(1, 2, rdd)
    val result = binnedRdd.take(10)

    // Validate
    result.apply(0) shouldBe Array[Any]("A", 1, 0)
    result.apply(1) shouldBe Array[Any]("B", 2, 0)
    result.apply(2) shouldBe Array[Any]("C", 3, 0)
    result.apply(3) shouldBe Array[Any]("D", 4, 0)
    result.apply(4) shouldBe Array[Any]("E", 5, 0)
    result.apply(5) shouldBe Array[Any]("F", 6, 1)
    result.apply(6) shouldBe Array[Any]("G", 7, 1)
    result.apply(7) shouldBe Array[Any]("H", 8, 1)
    result.apply(8) shouldBe Array[Any]("I", 9, 1)
    result.apply(9) shouldBe Array[Any]("J", 10, 1)
  }

  "binEqualDepth" should "put each value in separate bin if num_bins is greater than length of column" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5),
      Array[Any]("F", 6),
      Array[Any]("G", 7),
      Array[Any]("H", 8),
      Array[Any]("I", 9),
      Array[Any]("J", 10))
    val rdd = sc.parallelize(inputList, 2)

    // Get binned results
    val binnedRdd = SparkOps.binEqualDepth(1, 20, rdd)
    val result = binnedRdd.take(10)

    // Validate
    result.apply(0) shouldBe Array[Any]("A", 1, 0)
    result.apply(1) shouldBe Array[Any]("B", 2, 1)
    result.apply(2) shouldBe Array[Any]("C", 3, 2)
    result.apply(3) shouldBe Array[Any]("D", 4, 3)
    result.apply(4) shouldBe Array[Any]("E", 5, 4)
    result.apply(5) shouldBe Array[Any]("F", 6, 5)
    result.apply(6) shouldBe Array[Any]("G", 7, 6)
    result.apply(7) shouldBe Array[Any]("H", 8, 7)
    result.apply(8) shouldBe Array[Any]("I", 9, 8)
    result.apply(9) shouldBe Array[Any]("J", 10, 9)
  }

}
