package com.intel.intelanalytics.engine.spark.frame.plugins.groupby.aggregators

import com.intel.intelanalytics.domain.schema.DataTypes
import org.scalatest.{ FlatSpec, Matchers }

class MaxAggregatorTest extends FlatSpec with Matchers {
  "MaxAggregator" should "output column value" in {
    val aggregator = MaxAggregator()

    aggregator.mapFunction("test1", DataTypes.string) should equal("test1")
    aggregator.mapFunction(10L, DataTypes.int64) should equal(10L)
  }
  "MaxAggregator" should "return the maximum value in" in {
    val aggregator = MaxAggregator()

    aggregator.add(10, 15) should equal(15)
    aggregator.add(-4, -10) should equal(-4)
    aggregator.add(100, 0) should equal(100)
    aggregator.add("test1", "abc") should equal("test1")
  }
  "MaxAggregator" should "merge two maximum values" in {
    val aggregator = MaxAggregator()

    aggregator.merge(23, 15) should equal(23)
    aggregator.merge(67, -10) should equal(67)
    aggregator.merge(100, 0) should equal(100)
    aggregator.merge("abc", "def") should equal("def")
  }
  "MaxAggregator" should "ignore null values" in {
    val aggregator = MaxAggregator()

    aggregator.add(100, null) should equal(100)
    aggregator.add(null, 10) should equal(10)
    aggregator.merge(-10, null) should equal(-10)
    aggregator.merge(null, 30) should equal(30)
  }
}
