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

import com.intel.intelanalytics.domain.schema.{ Column, DataTypes }
import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers

/**
 * Tests the entropy functions.
 *
 * The expected values for the tests were computed using the R entropy package.
 * @see http://cran.r-project.org/web/packages/entropy/index.html
 */
class EntropyITest extends TestingSparkContextFlatSpec with Matchers {
  val unweightedInput = List(
    Array[Any](-1, "a", 0),
    Array[Any](0, "a", 0),
    Array[Any](0, "b", 0),
    Array[Any](1, "b", 0),
    Array[Any](1, "b", 0),
    Array[Any](2, "c", 0))

  val weightedInput = List(
    Array[Any]("a", 1.0),
    Array[Any]("a", 1.0),
    Array[Any]("b", 0.8),
    Array[Any]("b", 0.3),
    Array[Any]("c", 0.2),
    Array[Any]("c", 0.1))

  val emptyList = List.empty[Array[Any]]

  val epsilon = 0.000001
  "shannonEntropy" should "compute the correct shannon entropy for unweighted data" in {
    val rowRDD = sparkContext.parallelize(unweightedInput, 2)
    val entropy1 = EntropyRDDFunctions.shannonEntropy(rowRDD, 0)
    val entropy2 = EntropyRDDFunctions.shannonEntropy(rowRDD, 1)
    val entropy3 = EntropyRDDFunctions.shannonEntropy(rowRDD, 2)

    // Expected values were computed using the entropy.empirical method in the R entropy package
    // Input to entropy.empirical is an array of counts of distinct values
    entropy1 should equal(1.329661 +- epsilon) //entropy.empirical(c(1, 2, 2, 1), 'log')
    entropy2 should equal(1.011404 +- epsilon) //entropy.empirical(c(2,3,1), 'log')
    entropy3 should equal(0)
  }
  "shannonEntropy" should "compute the correct shannon entropy for weighted data" in {
    val rowRDD = sparkContext.parallelize(weightedInput, 2)
    val column = Column("columnName", DataTypes.float64)
    column.index = 1
    val entropy = EntropyRDDFunctions.shannonEntropy(rowRDD, 0, Some(column))

    // Expected values were computed using the entropy.empirical method in the R entropy package
    // Input to entropy.empirical is an array of sums of weights of distinct values
    entropy should equal(0.891439 +- epsilon) //entropy.empirical(c(2, 1.1, 0.3), 'log')
  }
  "shannonEntropy" should "should return zero if frame is empty" in {
    val frameRdd = sparkContext.parallelize(emptyList, 2)
    val entropy = EntropyRDDFunctions.shannonEntropy(frameRdd, 0)
    entropy should equal(0)
  }
}
