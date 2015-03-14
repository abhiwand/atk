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

package com.intel.intelanalytics.engine.spark.frame.plugins.dotproduct

import com.intel.intelanalytics.domain.schema.{ Column, DataTypes, FrameSchema }
import org.apache.spark.frame.FrameRdd
import com.intel.testutils.MatcherUtils._
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers

class DotProductFunctionsTest extends TestingSparkContextFlatSpec with Matchers {
  val epsilon = 0.000000001

  val inputRows: Array[sql.Row] = Array(
    new GenericRow(Array[Any](1d, 0.2d, -2, 5, "1,2", "3,4")),
    new GenericRow(Array[Any](2d, 0.4d, -1, 6, "5,6", "7,8")),
    new GenericRow(Array[Any](3d, 0.6d, 0, 7, "9,10", "11,12")),
    new GenericRow(Array[Any](4d, 0.8d, 1, 8, "-2,3", "-4,-5")),
    new GenericRow(Array[Any](5d, null, 2, null, "6, ", " ,7")),
    new GenericRow(Array[Any](null, null, null, null, null, null))
  )

  val inputSchema = FrameSchema(List(
    Column("col_0", DataTypes.float64),
    Column("col_1", DataTypes.float64),
    Column("col_2", DataTypes.int32),
    Column("col_3", DataTypes.int32),
    Column("col_4", DataTypes.str),
    Column("col_5", DataTypes.str)
  ))

  "dotProduct" should "compute the dot product for sequences of columns" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)

    val results = DotProductFunctions.dotProduct(frameRdd, List("col_0", "col_1"), List("col_2", "col_3")).collect()
    val dotProducts = results.map(row => row(6).asInstanceOf[Double])

    results.size should be(6)
    dotProducts should equalWithTolerance(Array(-1d, 0.4d, 4.2d, 10.4d, 10d, 0d), epsilon)
  }
  "dotProduct" should "compute the dot product for sequences of columns using defaults for nulls" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)

    val results = DotProductFunctions.dotProduct(frameRdd, List("col_0", "col_1"), List("col_2", "col_3"),
      Some(List(0.1, 0.2)), Some(List(0.3, 0.4))).collect()
    val dotProducts = results.map(row => row(6).asInstanceOf[Double])

    results.size should be(6)
    dotProducts should equalWithTolerance(Array(-1d, 0.4d, 4.2d, 10.4d, 10.08d, 0.11d), epsilon)
  }
  "dotProduct" should "compute the dot product for lists of doubles" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)

    val results = DotProductFunctions.dotProduct(frameRdd, List("col_4"), List("col_5")).collect()
    val dotProducts = results.map(row => row(6).asInstanceOf[Double])

    results.size should be(6)
    dotProducts should equalWithTolerance(Array(11d, 83d, 219d, -7d, 0d, 0d), epsilon)
  }
  "dotProduct" should "compute the dot product for lists of doubles using defaults for nulls" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)

    val results = DotProductFunctions.dotProduct(frameRdd, List("col_4"), List("col_5"),
      Some(List(0.1, 0.2)), Some(List(0.3, 0.4))).collect()
    val dotProducts = results.map(row => row(6).asInstanceOf[Double])

    results.size should be(6)
    dotProducts should equalWithTolerance(Array(11d, 83d, 219d, -7d, 3.2d, 0.11d), epsilon)
  }
  "computeDotProduct" should "compute the dot product" in {
    val leftVector = Seq(BigDecimal(1d), BigDecimal(2d), BigDecimal(3d))
    val rightVector = Seq(BigDecimal(4d), BigDecimal(5d), BigDecimal(6d))
    val dotProduct = DotProductFunctions.computeDotProduct(leftVector, rightVector)
    dotProduct should be(32d)
  }
  "computeDotProduct" should "throw an IllegalArgumentException if left vector is empty" in {
    intercept[IllegalArgumentException] {
      DotProductFunctions.computeDotProduct(Seq.empty[BigDecimal], Seq(BigDecimal(1d), BigDecimal(2d), BigDecimal(3d)))
    }
  }
  "computeDotProduct" should "throw an IllegalArgumentException if right vector is empty" in {
    intercept[IllegalArgumentException] {
      DotProductFunctions.computeDotProduct(Seq(BigDecimal(1d), BigDecimal(2d), BigDecimal(3d)), Seq.empty[BigDecimal])
    }
  }
  "computeDotProduct" should "throw an IllegalArgumentException if vectors are not the same size" in {
    intercept[IllegalArgumentException] {
      DotProductFunctions.computeDotProduct(Seq(BigDecimal(1d), BigDecimal(2d), BigDecimal(3d)),
        Seq(BigDecimal(1d), BigDecimal(2d), BigDecimal(3d), BigDecimal(4d)))
    }
  }

}
