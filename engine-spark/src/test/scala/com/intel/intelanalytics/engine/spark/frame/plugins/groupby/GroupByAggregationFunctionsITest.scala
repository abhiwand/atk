package com.intel.intelanalytics.engine.spark.frame.plugins.groupby

import com.intel.intelanalytics.domain.frame.GroupByAggregationArgs
import com.intel.intelanalytics.domain.schema.{ Column, DataTypes, FrameSchema }
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers

import scala.math.BigDecimal.RoundingMode
import scala.util.Try

class GroupByAggregationFunctionsITest extends TestingSparkContextFlatSpec with Matchers {

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
    val frameRDD = new FrameRDD(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(
      GroupByAggregationArgs("COUNT", "col_1", "col1_count"),
      GroupByAggregationArgs("SUM", "col_2", "col2_sum"),
      GroupByAggregationArgs("SUM", "col_1", "col1_sum"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRDD, groupByColumns, groupByArguments)
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
    val frameRDD = new FrameRDD(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("COUNT", "col_1", "col_count"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRDD, groupByColumns, groupByArguments)
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
    val frameRDD = new FrameRDD(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("COUNT_DISTINCT", "col_2", "col_distinct_count"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRDD, groupByColumns, groupByArguments)
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
    val frameRDD = new FrameRDD(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("MIN", "col_1", "col_min"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRDD, groupByColumns, groupByArguments)
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
    val frameRDD = new FrameRDD(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("MAX", "col_1", "col_max"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRDD, groupByColumns, groupByArguments)
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
    val frameRDD = new FrameRDD(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("SUM", "col_1", "col_sum"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRDD, groupByColumns, groupByArguments)
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
    val frameRDD = new FrameRDD(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("AVG", "col_2", "col_mean"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRDD, groupByColumns, groupByArguments)
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
    val rdd = sparkContext.parallelize(inputRows)
    val frameRDD = new FrameRDD(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("VAR", "col_2", "col_var"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRDD, groupByColumns, groupByArguments)
    val results = resultRDD.collect().map(row => {
      new GenericRow(Array[Any](row(0), Try(BigDecimal(row.getDouble(1)).setScale(9, RoundingMode.HALF_UP)).getOrElse("NaN")))
    })

    val expectedResults = List(
      new GenericRow(Array[Any]("a", BigDecimal(0.7d).setScale(9, RoundingMode.HALF_UP))),
      new GenericRow(Array[Any]("b", BigDecimal(1 / 3d).setScale(9, RoundingMode.HALF_UP))),
      new GenericRow(Array[Any]("c", "NaN"))
    )

    results should contain theSameElementsAs (expectedResults)

  }

}
