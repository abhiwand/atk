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

import com.intel.intelanalytics.domain.schema.{Column, DataTypes, FrameSchema, Schema}
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
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
   * Create a Summarized Frame with Aggregations (Avg, Count, Max, Min, Mean, Sum, Stdev, ...)
   *
   * @param frameRDD Input frame
   * @param groupByColumns List of columns to group by
   * @param aggregationArguments List of aggregation arguments (i.e., aggregation function, column, new column name)
   * @return Summarized frame with aggregations
   */
  def aggregation(frameRDD: FrameRDD,
                  groupByColumns: List[Column],
                  aggregationArguments: List[(String, String, String)]): FrameRDD = {

    val frameSchema = frameRDD.frameSchema

    val pairedRowRDD = pairRowsByGroupKey(frameRDD, groupByColumns, aggregationArguments)
    val aggregationColumns = getAggregationColumns(frameSchema, aggregationArguments)

    val aggregator = new GroupByAggregationByKey(pairedRowRDD, aggregationColumns, aggregationArguments)
    val aggregationRDD = aggregator.aggregateByKey()

    val newColumns = groupByColumns ++ aggregationColumns
    val newSchema = FrameSchema(newColumns)

    FrameRDD.toFrameRDD(newSchema, aggregationRDD)
  }

  /**
   * Get the columns used in the aggregation
   *
   * @param frameSchema Frame schema
   * @param aggregationArguments List of aggregation arguments (i.e., aggregation function, column, new column name)
   * @return List of aggregation columns
   */
  private def getAggregationColumns(frameSchema: Schema, aggregationArguments: List[(String, String, String)]): List[Column] = {
    aggregationArguments.map {
      case (aggregationFunction, columnName, newColumnName) =>
        val column = frameSchema.column(columnName)
        aggregationFunction match {
          case "COUNT" | "COUNT_DISTINCT" => Column(newColumnName, DataTypes.int64)
          case "MIN" | "MAX" => Column(newColumnName, column.dataType)
          case "SUM" => {
            val dataType = if (column.dataType == DataTypes.int32 || column.dataType == DataTypes.int64) DataTypes.int64 else DataTypes.float64
            Column(newColumnName, dataType)
          }
          case _ => Column(newColumnName, DataTypes.float64)
        }
    }
  }

  /**
   * Get an RDD with the group-by keys, and aggregation columns
   *
   * @param frameRDD Input frame
   * @param groupByColumns Group by column
   * @param aggregationArguments List of aggregation arguments (i.e., aggregation function, column, new column name)
   * @return RDD of group-by keys, and aggregation column values
   */
  private def pairRowsByGroupKey(frameRDD: FrameRDD, groupByColumns: List[Column], aggregationArguments: List[(String, String, String)]): RDD[(Seq[Any], Seq[Any])] = {
    val frameSchema = frameRDD.frameSchema
    val groupByColumnsNames = groupByColumns.map(col => col.name)

    val aggregationColumns = aggregationArguments.map {
      case (aggregationFunction, columnName, newColumnName) =>
        frameSchema.column(columnName = columnName)
    }

    frameRDD.mapRows(row => {
      val groupByKey = if (groupByColumns.length > 0) row.valuesAsArray(groupByColumnsNames).toSeq else Seq[Any]()
      val groupByRow = aggregationColumns.map(col => row.data(frameSchema.columnIndex(col.name)))
      (groupByKey, groupByRow.toSeq)
    })
  }

}
