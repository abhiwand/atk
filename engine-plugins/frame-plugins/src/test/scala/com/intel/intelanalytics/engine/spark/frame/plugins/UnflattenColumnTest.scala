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

package com.intel.intelanalytics.engine.spark.frame.plugins.unflattencolumn

import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import com.intel.intelanalytics.engine.spark.frame.plugins.{ UnflattenColumnFunctions }
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql
import org.scalatest.{ BeforeAndAfterEach, FlatSpec, Matchers }

class UnflattenColumnTest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContextFlatSpec {
  private val nameColumn = "name"
  private val dateColumn = "date"

  val dailyHeartbeats_4_1 = List(
    Array[Any]("Bob", "1/1/2015", "1", "60"),
    Array[Any]("Bob", "1/1/2015", "2", "70"),
    Array[Any]("Bob", "1/1/2015", "3", "65"),
    Array[Any]("Bob", "1/1/2015", "4", "55"))

  val dailyHeartbeats_1_1 = List(
    Array[Any]("Bob", "1/1/2015", "1", "60"))

  val dailyHeartbeats_2_2 = List(
    Array[Any]("Mary", "1/1/2015", "1", "60"),
    Array[Any]("Bob", "1/1/2015", "1", "60"))

  def executeTest(data: List[Array[Any]], rowsInResult: Int): Array[sql.Row] = {
    val schema = Schema.fromTuples(List((nameColumn, DataTypes.string),
      (dateColumn, DataTypes.string),
      ("minute", DataTypes.int32),
      ("heartRate", DataTypes.int32)))
    val compositeKeyColumnNames = List(nameColumn, dateColumn)
    val compositeKeyIndices = List(0, 1)

    val rows = sparkContext.parallelize(data)
    val rdd = FrameRdd.toFrameRdd(schema, rows).groupByRows(row => row.values(compositeKeyColumnNames))

    val targetSchema = UnflattenColumnFunctions.createTargetSchema(schema, compositeKeyColumnNames)
    val resultRdd = UnflattenColumnFunctions.unflattenRddByCompositeKey(compositeKeyIndices, rdd, targetSchema, ",")

    resultRdd.take(rowsInResult)
  }

  "UnflattenRddByCompositeKey::1" should "compress data in a single row" in {

    val rowInResult = 1
    val result = executeTest(dailyHeartbeats_4_1, rowInResult)

    assert(result.length == rowInResult)
    result.apply(rowInResult - 1) shouldBe Array[Any]("Bob", "1/1/2015", "1,2,3,4", "60,70,65,55")
  }

  "UnflattenRddByCompositeKey::2" should "compress data in a single row" in {

    val rowInResult = 1
    val result = executeTest(dailyHeartbeats_1_1, rowInResult)

    assert(result.length == rowInResult)
    result.apply(rowInResult - 1) shouldBe Array[Any]("Bob", "1/1/2015", "1", "60")
  }

  "UnflattenRddByCompositeKey::3" should "compress data in two rows" in {
    val rowInResult = 2
    val result = executeTest(dailyHeartbeats_2_2, rowInResult)

    assert(result.length == rowInResult)
  }
}

