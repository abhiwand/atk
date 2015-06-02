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
import com.intel.intelanalytics.domain.schema.{ Column, DataTypes, FrameSchema, Schema }
import com.intel.intelanalytics.engine.spark.frame.plugins.groupby.aggregators._
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD

/**
 * Aggregations for Frames (SUM, COUNT, etc)
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
private[spark] object GroupByAggregationFunctions extends Serializable {

  /**
   * Create a Summarized Frame with Aggregations (Avg, Count, Max, Min, ...).
   *
   * For example, grouping a frame by gender and age, and computing the average income.
   *
   * New aggregations can be added by implementing a GroupByAggregator.
   *
   * @see GroupByAggregator
   *
   * @param frameRdd Input frame
   * @param groupByColumns List of columns to group by
   * @param aggregationArguments List of aggregation arguments
   * @return Summarized frame with aggregations
   */
  def aggregation(frameRdd: FrameRdd,
                  groupByColumns: List[Column],
                  aggregationArguments: List[GroupByAggregationArgs]): FrameRdd = {

    val frameSchema = frameRdd.frameSchema
    val columnAggregators = createColumnAggregators(frameSchema, aggregationArguments)

    val pairedRowRDD = pairRowsByGroupByColumns(frameRdd, groupByColumns, aggregationArguments)

    val aggregationRDD = GroupByAggregateByKey(pairedRowRDD, columnAggregators).aggregateByKey()

    val newColumns = groupByColumns ++ columnAggregators.map(_.column)
    val newSchema = FrameSchema(newColumns)

    FrameRdd.toFrameRdd(newSchema, aggregationRDD)
  }

  /**
   * Returns a list of columns and corresponding accumulators used to aggregate values
   *
   * @param aggregationArguments List of aggregation arguments (i.e., aggregation function, column, new column name)
   * @param frameSchema Frame schema
   * @return  List of columns and corresponding accumulators
   */
  def createColumnAggregators(frameSchema: Schema, aggregationArguments: List[(GroupByAggregationArgs)]): List[ColumnAggregator] = {

    aggregationArguments.zipWithIndex.map {
      case (arg, i) =>
        val column = frameSchema.column(arg.columnName)

        arg.function match {
          case "COUNT" => {
            ColumnAggregator(Column(arg.newColumnName, DataTypes.int64), i, CountAggregator())
          }
          case "COUNT_DISTINCT" => {
            ColumnAggregator(Column(arg.newColumnName, DataTypes.int64), i, DistinctCountAggregator())
          }
          case "MIN" => {
            ColumnAggregator(Column(arg.newColumnName, column.dataType), i, MinAggregator())
          }
          case "MAX" => {
            ColumnAggregator(Column(arg.newColumnName, column.dataType), i, MaxAggregator())
          }
          case "SUM" if column.dataType.isNumerical => {
            if (column.dataType.isInteger)
              ColumnAggregator(Column(arg.newColumnName, DataTypes.int64), i, new SumAggregator[Long]())
            else
              ColumnAggregator(Column(arg.newColumnName, DataTypes.float64), i, new SumAggregator[Double]())
          }
          case "AVG" if column.dataType.isNumerical => {
            ColumnAggregator(Column(arg.newColumnName, DataTypes.float64), i, MeanAggregator())
          }
          case "VAR" if column.dataType.isNumerical => {
            ColumnAggregator(Column(arg.newColumnName, DataTypes.float64), i, VarianceAggregator())
          }
          case "STDEV" if column.dataType.isNumerical => {
            ColumnAggregator(Column(arg.newColumnName, DataTypes.float64), i, StandardDeviationAggregator())
          }
          case function if function.matches("""HISTOGRAM.*""") && column.dataType.isNumerical => {
            ColumnAggregator.getHistogramColumnAggregator(arg, i)
          }
          case _ => throw new IllegalArgumentException(s"Unsupported aggregation function: ${arg.function} for data type: ${column.dataType}")
        }
    }
  }

  /**
   * Create a pair RDD using the group-by keys, and aggregation columns
   *
   * The group-by key is a sequence of column values, for example, group-by gender and age. The aggregation
   * columns are the columns containing the values to be aggregated, for example, annual income.
   *
   * @param frameRdd Input frame
   * @param groupByColumns Group by columns
   * @param aggregationArguments List of aggregation arguments
   * @return RDD of group-by keys, and aggregation column values
   */
  def pairRowsByGroupByColumns(frameRdd: FrameRdd,
                               groupByColumns: List[Column],
                               aggregationArguments: List[GroupByAggregationArgs]): RDD[(Seq[Any], Seq[Any])] = {
    val frameSchema = frameRdd.frameSchema
    val groupByColumnsNames = groupByColumns.map(col => col.name)

    val aggregationColumns = aggregationArguments.map(arg => frameSchema.column(columnName = arg.columnName))

    frameRdd.mapRows(row => {
      val groupByKey = if (!groupByColumnsNames.isEmpty) row.valuesAsArray(groupByColumnsNames).toSeq else Seq[Any]()
      val groupByRow = aggregationColumns.map(col => row.data(frameSchema.columnIndex(col.name)))
      (groupByKey, groupByRow.toSeq)
    })
  }

}