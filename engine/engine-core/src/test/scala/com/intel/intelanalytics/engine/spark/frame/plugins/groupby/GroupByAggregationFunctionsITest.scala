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

package com.intel.intelanalytics.engine.spark.frame.plugins.groupby

import com.intel.intelanalytics.domain.frame.GroupByAggregationArgs
import com.intel.intelanalytics.domain.schema.{ Column, DataTypes, FrameSchema }
import org.apache.spark.frame.FrameRdd
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import com.intel.testutils.MatcherUtils._

import scala.math.BigDecimal.RoundingMode

class GroupByAggregationFunctionsITest extends TestingSparkContextFlatSpec with Matchers {
  val epsilon = 0.000001

  val inputRows: Array[sql.Row] = Array(
    new GenericRow(Array[Any]("a", 1, 1d, "w")),
    new GenericRow(Array[Any]("a", 2, 1d, "x")),
    new GenericRow(Array[Any]("a", 3, 2d, "x")),
    new GenericRow(Array[Any]("a", 4, 2d, "y")),
    new GenericRow(Array[Any]("a", 5, 3d, "z")),
    new GenericRow(Array[Any]("b", -1, 1d, "1")),
    new GenericRow(Array[Any]("b", 0, 1d, "2")),
    new GenericRow(Array[Any]("b", 1, 2d, "3")),
    new GenericRow(Array[Any]("b", 2, null, "4")),
    new GenericRow(Array[Any]("c", 5, 1d, "5"))
  )

  val inputSchema = FrameSchema(List(
    Column("col_0", DataTypes.string),
    Column("col_1", DataTypes.int32),
    Column("col_2", DataTypes.float64),
    Column("col_3", DataTypes.string)
  ))

  "Multi" should "count and sum the number of values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(
      GroupByAggregationArgs("COUNT", "col_1", "col1_count"),
      GroupByAggregationArgs("SUM", "col_2", "col2_sum"),
      GroupByAggregationArgs("SUM", "col_1", "col1_sum"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect()

    val expectedResults = List(
      new GenericRow(Array[Any]("a", 5, 9d, 15)),
      new GenericRow(Array[Any]("b", 4, 4d, 2)),
      new GenericRow(Array[Any]("c", 1, 1d, 5))
    )

    results should contain theSameElementsAs (expectedResults)
  }
  "COUNT" should "count the number of values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("COUNT", "col_1", "col_count"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect()

    val expectedResults = List(
      new GenericRow(Array[Any]("a", 5)),
      new GenericRow(Array[Any]("b", 4)),
      new GenericRow(Array[Any]("c", 1))
    )

    results.size shouldBe 3
    results should contain theSameElementsAs (expectedResults)
  }

  "COUNT_DISTINCT" should "count the number of distinct values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("COUNT_DISTINCT", "col_2", "col_distinct_count"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect()

    val expectedResults = List(
      new GenericRow(Array[Any]("a", 3)),
      new GenericRow(Array[Any]("b", 3)),
      new GenericRow(Array[Any]("c", 1))
    )

    results should contain theSameElementsAs (expectedResults)
  }

  "MIN" should "return the minimum values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("MIN", "col_1", "col_min"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect()

    val expectedResults = List(
      new GenericRow(Array[Any]("a", 1)),
      new GenericRow(Array[Any]("b", -1)),
      new GenericRow(Array[Any]("c", 5))
    )

    results should contain theSameElementsAs (expectedResults)
  }

  "MAX" should "return the maximum values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("MAX", "col_1", "col_max"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect()

    val expectedResults = List(
      new GenericRow(Array[Any]("a", 5)),
      new GenericRow(Array[Any]("b", 2)),
      new GenericRow(Array[Any]("c", 5))
    )

    results should contain theSameElementsAs (expectedResults)
  }

  "SUM" should "return the sum of values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("SUM", "col_1", "col_sum"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect()

    val expectedResults = List(
      new GenericRow(Array[Any]("a", 15)),
      new GenericRow(Array[Any]("b", 2)),
      new GenericRow(Array[Any]("c", 5))
    )

    results should contain theSameElementsAs (expectedResults)
  }

  "AVG" should "return the arithmetic mean of values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("AVG", "col_2", "col_mean"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect().map(row => {
      new GenericRow(Array[Any](row(0), BigDecimal(row.getDouble(1)).setScale(9, RoundingMode.HALF_UP)))
    })

    val expectedResults = List(
      new GenericRow(Array[Any]("a", BigDecimal(1.8d).setScale(9, RoundingMode.HALF_UP))),
      new GenericRow(Array[Any]("b", BigDecimal(4 / 3d).setScale(9, RoundingMode.HALF_UP))),
      new GenericRow(Array[Any]("c", BigDecimal(1d).setScale(9, RoundingMode.HALF_UP)))
    )

    results should contain theSameElementsAs (expectedResults)
  }

  "VAR" should "return the variance of values by key" in {
    val rdd = sparkContext.parallelize(inputRows, 3)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("VAR", "col_2", "col_var"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect().map(row => {
      val variance = if (row(1) == null) null else BigDecimal(row.getDouble(1)).setScale(9, RoundingMode.HALF_UP)
      new GenericRow(Array[Any](row(0), variance))
    })

    val expectedResults = List(
      new GenericRow(Array[Any]("a", BigDecimal(0.7d).setScale(9, RoundingMode.HALF_UP))),
      new GenericRow(Array[Any]("b", BigDecimal(1 / 3d).setScale(9, RoundingMode.HALF_UP))),
      new GenericRow(Array[Any]("c", null))
    )

    results should contain theSameElementsAs (expectedResults)

  }
  "HISTOGRAM" should "return the histogram of values by key" in {
    val rdd = sparkContext.parallelize(inputRows, 3)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("HISTOGRAM={\"cutoffs\": [0,2,4] }", "col_2", "col_histogram"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.map(row => (row(0), row(1).asInstanceOf[Vector[Double]].toArray)).collect().toMap

    results("a") should equalWithTolerance(Array(0.4d, 0.6d), epsilon)
    results("b") should equalWithTolerance(Array(2 / 3d, 1 / 3d), epsilon)
    results("c") should equalWithTolerance(Array(1d, 0d), epsilon)
  }

}