package com.intel.intelanalytics.engine.spark.frame.plugins.groupby.aggregators

import com.intel.intelanalytics.domain.schema.DataTypes
import org.scalatest.{ FlatSpec, Matchers }

class CountAggregatorTest extends FlatSpec with Matchers {
  "CountAggregator" should "output 'one' for each column value" in {
    val aggregator = CountAggregator()

    aggregator.mapFunction("test", DataTypes.string) should equal(1L)
    aggregator.mapFunction(23d, DataTypes.float64) should equal(1L)
  }
  "CountAggregator" should "increment count" in {
    val aggregator = CountAggregator()

    aggregator.add(20L, 1L) should equal(21L)
    aggregator.add(20L, 2L) should equal(22L)
  }
  "CountAggregator" should "sum two counts" in {
    val aggregator = CountAggregator()

    aggregator.merge(5L, 100L) should equal(105L)
  }

}
