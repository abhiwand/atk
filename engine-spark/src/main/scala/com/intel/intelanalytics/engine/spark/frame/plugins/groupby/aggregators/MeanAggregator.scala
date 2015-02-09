package com.intel.intelanalytics.engine.spark.frame.plugins.groupby.aggregators

import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes.DataType

/**
 * Counter used to compute the arithmetic mean incrementally.
 */
case class MeanCounter(count: Long, sum: Double) {
  require(count >= 0, "Count should be greater than zero")
}

/**
 *  Aggregator for incrementally computing the mean column value using Spark's aggregateByKey()
 *
 *  @see org.apache.spark.rdd.PairRDDFunctions#aggregateByKey
 */
case class MeanAggregator() extends GroupByAggregator {

  /** Type for aggregate values that corresponds to type U in Spark's aggregateByKey() */
  override type AggregateType = MeanCounter

  /** Output type of the map function that corresponds to type V in Spark's aggregateByKey() */
  override type ValueType = Double

  /** The 'empty' or 'zero' or default value for the aggregator */
  override def zero: MeanCounter = MeanCounter(0L, 0d)

  /**
   * Converts column value to Double
   */
  override def mapFunction(columnValue: Any, columnDataType: DataType): ValueType = {
    if (columnValue != null)
      DataTypes.toDouble(columnValue)
    else
      Double.NaN
  }

  /**
   * Adds map value to incremental mean counter
   */
  override def add(mean: AggregateType, mapValue: ValueType): AggregateType = {
    if (mapValue.isNaN) { // omit value from calculation
      //TODO: Log to IAT EventContext once we figure out how to pass it to Spark workers
      println(s"WARN: Omitting NaNs from mean calculation in group-by")
      mean
    }
    else {
      val sum = mean.sum + mapValue
      val count = mean.count + 1L
      MeanCounter(count, sum)
    }
  }

  /**
   * Merge two mean counters
   */
  override def merge(mean1: AggregateType, mean2: AggregateType) = {
    val count = mean1.count + mean2.count
    val sum = mean1.sum + mean2.sum
    MeanCounter(count, sum)
  }
  override def getResult(result: AggregateType): Any = result.sum / result.count

}
