package com.intel.intelanalytics.engine.spark.frame.plugins.groupby.aggregators

import com.intel.intelanalytics.domain.schema.DataTypes
import org.scalatest.{ Matchers, FlatSpec }

class DistinctCountAggregatorTest extends FlatSpec with Matchers {
  "DistinctCountAggregator" should "output column value" in {
    val aggregator = DistinctCountAggregator()

    aggregator.mapFunction("test1", DataTypes.string) should equal("test1")
    aggregator.mapFunction(10L, DataTypes.float64) should equal(10L)

  }
  "DistinctCountAggregator" should "add value to set" in {
    val aggregator = DistinctCountAggregator()
    val set: Set[Any] = Set("test1", "test2", "test3")
    aggregator.add(set, "test4") should contain theSameElementsAs Set("test1", "test2", "test3", "test4")
    aggregator.add(set, "test1") should contain theSameElementsAs Set("test1", "test2", "test3")
  }
  "DistinctCountAggregator" should "merge two sets" in {
    val aggregator = DistinctCountAggregator()

    val set1: Set[Any] = Set("test1", "test2", "test3")
    val set2: Set[Any] = Set(1, 2, 4)
    aggregator.merge(set1, set2) should contain theSameElementsAs Set("test1", "test2", "test3", 1, 2, 4)
    aggregator.merge(Set.empty[Any], set2) should contain theSameElementsAs Set(1, 2, 4)
  }
  "DistinctCountAggregator" should "return count of distinct values" in {
    val aggregator = DistinctCountAggregator()

    val set1: Set[Any] = Set("test1", "test2", "test3")
    aggregator.getResult(set1) should equal(3)
    aggregator.getResult(Set.empty[Any]) should equal(0)
  }

}
