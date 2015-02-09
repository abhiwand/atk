package com.intel.intelanalytics.engine.spark.frame.plugins.groupby.aggregators

import com.intel.intelanalytics.domain.schema.DataTypes
import org.scalatest.{ Matchers, FlatSpec }

class MinAggregatorTest extends FlatSpec with Matchers {
  "MinAggregator" should "output column value" in {
    val aggregator = MinAggregator()

    aggregator.mapFunction("test1", DataTypes.string) should equal("test1")
    aggregator.mapFunction(10L, DataTypes.int64) should equal(10L)

  }
  "MinAggregator" should "return the minimum value in" in {
    val aggregator = MinAggregator()

    aggregator.add(10, 15) should equal(10)
    aggregator.add(-4, -10) should equal(-10)
    aggregator.add(100, 0) should equal(0)
    aggregator.add("test1", "abc") should equal("abc")
  }
  "MinAggregator" should "merge two minimum values" in {
    val aggregator = MinAggregator()

    aggregator.merge(23, 15) should equal(15)
    aggregator.merge(67, -10) should equal(-10)
    aggregator.merge(100, 0) should equal(0)
    aggregator.merge("abc", "def") should equal("abc")
  }
  "MinAggregator" should "ignore null values" in {
    val aggregator = MinAggregator()

    aggregator.add(100, null) should equal(100)
    aggregator.add(null, 10) should equal(10)
    aggregator.merge(-10, null) should equal(-10)
    aggregator.merge(null, 30) should equal(30)
  }

}
