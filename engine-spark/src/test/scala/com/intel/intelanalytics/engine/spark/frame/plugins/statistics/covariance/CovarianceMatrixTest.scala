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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.covariance

import com.intel.intelanalytics.domain.schema.{ Column, FrameSchema, DataTypes }
import org.apache.spark.frame.FrameRdd
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import com.intel.intelanalytics.engine.Rows._

class CovarianceMatrixTest extends TestingSparkContextFlatSpec with Matchers {
  val inputArray: Array[Array[Double]] = Array(Array(90.0, 60.0, 90.0), Array(90.0, 90.0, 30.0),
    Array(60.0, 60.0, 60.0), Array(60.0, 60.0, 90.0), Array(30.0, 30.0, 30.0))

  "CovarianceFunctions matrix calculations" should "return the correct values" in {

    val arrGenericRow: Array[sql.Row] = inputArray.map(row => {
      val temp: Array[Any] = row.map(x => x)
      new GenericRow(temp)
    })

    val rdd = sparkContext.parallelize(arrGenericRow)
    val columnsList = List("col_0", "col_1", "col_2")
    val inputDataColumnNamesAndTypes: List[Column] = columnsList.map({ name => Column(name, DataTypes.float64) }).toList
    val schema = FrameSchema(inputDataColumnNamesAndTypes)
    val frameRdd = new FrameRdd(schema, rdd)
    val result = CovarianceFunctions.covarianceMatrix(frameRdd, columnsList).collect()
    result.size shouldBe 3
    result(0) shouldBe Array(630.0, 450.0, 225.0)
    result(1) shouldBe Array(450.0, 450.0, 0.0)
    result(2) shouldBe Array(225.0, 0.0, 900.0)
  }
  "CovarianceFunctions matrix calculations" should "return the correct values for vector data types" in {
    val arrGenericRow: Array[sql.Row] = inputArray.map(row => {
      val temp: Array[Any] = Array(DataTypes.toVector(3)(row))
      new GenericRow(temp)
    })

    val rdd = sparkContext.parallelize(arrGenericRow)
    val schema = FrameSchema(List(Column("col_0", DataTypes.vector(3))))
    val frameRdd = new FrameRdd(schema, rdd)
    val result = CovarianceFunctions.covarianceMatrix(frameRdd, List("col_0"), outputVectorLength = Some(3)).collect()

    result.size shouldBe 3
    result(0)(0) shouldBe Vector(630.0, 450.0, 225.0)
    result(1)(0) shouldBe Vector(450.0, 450.0, 0.0)
    result(2)(0) shouldBe Vector(225.0, 0.0, 900.0)
  }
  "CovarianceFunctions matrix calculations" should "return the correct values for mixed vector and numeric data types" in {
    val arrGenericRow: Array[sql.Row] = inputArray.map(row => {
      val temp: Array[Any] = Array(DataTypes.toVector(2)(row.slice(0, 2)), row(2))
      new GenericRow(temp)
    })

    val rdd = sparkContext.parallelize(arrGenericRow)
    val schema = FrameSchema(List(Column("col_0", DataTypes.vector(2)), Column("col_1", DataTypes.float64)))
    val frameRdd = new FrameRdd(schema, rdd)
    val result = CovarianceFunctions.covarianceMatrix(frameRdd, List("col_0", "col_1")).collect()

    result.size shouldBe 3
    result(0) shouldBe Array(630.0, 450.0, 225.0)
    result(1) shouldBe Array(450.0, 450.0, 0.0)
    result(2) shouldBe Array(225.0, 0.0, 900.0)
  }

}
