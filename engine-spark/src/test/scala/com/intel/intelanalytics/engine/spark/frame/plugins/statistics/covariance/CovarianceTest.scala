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
import org.apache.spark.frame.FrameRDD
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import com.intel.intelanalytics.engine.Rows._

class CovarianceMatrixTest extends TestingSparkContextFlatSpec with Matchers {
  "Covariance matrix calculations" should "return the correct values" in {

    val inputArray: Array[Array[Double]] = Array(Array(90.0, 60.0, 90.0), Array(90.0, 90.0, 30.0), Array(60.0, 60.0, 60.0), Array(60.0, 60.0, 90.0), Array(30.0, 30.0, 30.0))

    val arrGenericRow: Array[sql.Row] = inputArray.map(row => {
      val temp: Array[Any] = row.map(x => x)
      new GenericRow(temp)
    })

    val rdd = sparkContext.parallelize(arrGenericRow)
    val columnsList = List("col_0", "col_1", "col_2")
    val inputDataColumnNamesAndTypes: List[Column] = columnsList.map({ name => Column(name, DataTypes.float64) }).toList
    val schema = FrameSchema(inputDataColumnNamesAndTypes)
    val frameRDD = new FrameRDD(schema, rdd)
    val result = Covariance.covarianceMatrix(frameRDD, columnsList).collect()
    result.size shouldBe 3
    result(0) shouldBe Array(630.0, 450.0, 225.0)
    result(1) shouldBe Array(450.0, 450.0, 0.0)
    result(2) shouldBe Array(225.0, 0.0, 900.0)
  }
}
