//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

import com.intel.intelanalytics.domain.schema.{Column, DataTypes, FrameSchema}
import com.intel.intelanalytics.engine.Rows
import com.intel.intelanalytics.engine.spark.frame.plugins.groupby.GroupByAccumulators._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * Computes the aggregated values (Avg, Count, Max, Min, Mean, Sum, Stdev, ...) for specified columns grouped by key.
 *
 * This class uses Spark's aggregateByKey() transformation to compute the aggregated values.
 * aggregateByKey() uses a combine function and a neutral "zero value" to accumulate values such as counts.
 *
 * @param pairedRDD RDD of group-by keys, and aggregation column values
 * @param aggregationColumns Columns to aggregate
 * @param aggregationArguments List of aggregation arguments (i.e., aggregation function, column, new column name)
 */
class GroupByAggregationByKey(pairedRDD: RDD[(Seq[Any], Seq[Any])],
                              aggregationColumns: List[Column],
                              aggregationArguments: List[(String, String, String)]) extends Serializable {

  type U = (aggregator#U) forSome {type aggregator >: GroupByAccumulator <: GroupByAccumulator}

  val numAggregationColumns = aggregationColumns.length
  val columnAccumulators = getColumnAccumulators(aggregationArguments, aggregationColumns)
  val accumulators = columnAccumulators.map(_.accumulator).toArray
  val initialAccumulatorValues = accumulators.map(_.zero)


  /**
   * Computes the aggregated values (Avg, Count, Max, Min, Mean, Sum, Stdev, ...) for specified columns grouped by key.
   *
   * @return Row RDD with results of aggregation
   */
  def aggregateByKey(): RDD[Rows.Row] = {
    pairedRDD.map { case (key, row) =>
      mapValues(key, row, columnAccumulators)
    }
      .aggregateByKey[Seq[Any]](initialAccumulatorValues)(
        (accumulatorValues, columnValues) => accumulateValues(accumulatorValues, columnValues),
        (accumulatorValues1, accumulatorValues2) => combineAccumulators(accumulatorValues1, accumulatorValues2)
      )
      .map { case (key, row) =>
      getResults(key, row)
    }
  }

  /**
   * Returns a list of columns and corresponding accumulators used to aggregate values
   *
   * @param aggregationArguments List of aggregation arguments (i.e., aggregation function, column, new column name)
   * @param aggregationColumns Columns to aggregate
   * @return  List of columns and corresponding accumulators 
   */
  private def getColumnAccumulators(aggregationArguments: List[(String, String, String)], aggregationColumns: List[Column]): List[ColumnAccumulator] = {
    val aggregationSchema = FrameSchema(aggregationColumns)

    aggregationArguments.map {
      case (aggregationFunction, columnName, newColumnName) =>
        //using copy to prevent column indices from changing if spark re-orders operations and FrameSchema gets called
        val column = aggregationSchema.column(newColumnName).copy()
        aggregationFunction match {
          case "COUNT" => ColumnAccumulator(column, CountAccumulator)
          case "COUNT_DISTINCT" => ColumnAccumulator(column, DistinctCountAccumulator)
          case "MIN" => ColumnAccumulator(column, MinAccumulator)
          case "MAX" => ColumnAccumulator(column, MaxAccumulator)
          case "SUM" => {
            if (DataTypes.isIntegral(column.dataType))
              ColumnAccumulator(column, new SumAccumulator[Long]())
            else
              ColumnAccumulator(column, new SumAccumulator[Double]())
          }
          case "AVG" => ColumnAccumulator(column, MeanAccumulator)
          case "VAR" => ColumnAccumulator(column, VarianceAccumulator)
          case "STDEV" => ColumnAccumulator(column, StandardDeviationAccumulator)
          case _ => throw new IllegalArgumentException(s"Unsupported aggregation function: ${aggregationFunction}")
        }
    }
  }

  /**
   * Applies the map function for accumulator to the corresponding columns in a row.
   *
   * For example, the map function for the CountAccumulator outputs (key, 1L)
   *
   * @param key Group key
   * @param row Row of column values to aggregate
   * @param columnAccumulators List of columns and corresponding accumulators used to aggregate values
   * @return Group key, and sequence of accumulator values
   */
  private def mapValues(key: Seq[Any], row: Seq[Any], columnAccumulators: Seq[ColumnAccumulator]): (Seq[Any], Seq[GroupByAccumulator#V]) = {
    val seq = columnAccumulators.map(columnAccumulator => {
      val columnIndex = columnAccumulator.column.index
      val columnDataType = columnAccumulator.column.dataType
      columnAccumulator.accumulator.mapValue(row(columnIndex), columnDataType)
    })
    (key, seq)
  }


  /**
   * Accumulates column values for a given key
   *
   * @param accumulatorValues Accumulated values for columns (e.g., running counts, averages)
   * @param columnValues Column values from Map stage
   * @return  Updated accumulated values
   */
  private def accumulateValues(accumulatorValues: Seq[Any],
                       columnValues: Seq[GroupByAccumulator#V]): Seq[U] = {
    var i = 0
    val buf = new ListBuffer[U]()

    // Using while loops instead of for loops to improve performance
    while (i < numAggregationColumns) {
      val aggregator = accumulators(i)
      val result = accumulatorValues(i).asInstanceOf[aggregator.type#U]
      val columnValue = columnValues(i).asInstanceOf[aggregator.type#V]
      buf += aggregator.accumulateValue(result, columnValue).asInstanceOf[U]
      i += 1
    }
    buf.toSeq
  }

  /**
   * Combine accumulated values for a given key from different Spark partitions
   *
   * For example, combining the running counts from different Spark partitions for a given key
   *
   * @param accumulatorValues1 Accumulator values for a given key from one Spark partition
   * @param accumulatorValues2 Accumulator values for a given key from a different Spark partition
   *
   * @return Combined accumulator values
   */
  private def combineAccumulators(accumulatorValues1: Seq[Any],
                          accumulatorValues2: Seq[Any]): Seq[U] = {
    var i = 0
    val buf = new ListBuffer[U]()

    // Using while loops instead of for loops to improve performance
    while (i < numAggregationColumns) {
      val aggregator = accumulators(i)

      // Need to check for empty accumulator values in case one of the Spark partitions had no data
      // Empty Spark partitions can arise if there are too many partitions
      val result1 = if (accumulatorValues1.isEmpty) accumulators(i).zero else accumulatorValues1(i)
      val result2 = if (accumulatorValues2.isEmpty) accumulators(i).zero else accumulatorValues2(i)
      buf += aggregator.mergeAccumulators(result1.asInstanceOf[aggregator.type#U], result2.asInstanceOf[aggregator.type#U]).asInstanceOf[U]
      i += 1
    }
    buf.toSeq
  }

  /**
   * Get the final results for a given key, e.g., total counts
   *
   * The output of the accumulators might be an intermediate result. For example, for the
   * arithmetic result, the output of the accumulator is the total sum and count. This method
   * computes the final output value (e.g., mean=(sum/count)
   *
   * @param key Row key
   * @param row List of aggregated values for each column
   * @return Row with key and aggregated values
   */
  private def getResults(key: Seq[Any], row: Seq[Any]): Rows.Row = {
    var i = 0
    val buf = new ListBuffer[Any]()
    buf ++= key
    while (i < row.length) {
      val columnValue = row(i)
      val aggregator = accumulators(i)
      buf += aggregator.getResult(columnValue.asInstanceOf[aggregator.type#U])
      i += 1
    }
    buf.toArray
  }

}
