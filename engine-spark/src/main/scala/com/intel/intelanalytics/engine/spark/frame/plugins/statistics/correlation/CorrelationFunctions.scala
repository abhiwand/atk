
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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.correlation

import breeze.linalg.DenseVector
import com.intel.intelanalytics.domain.DoubleValue
import com.intel.intelanalytics.domain.DoubleValue
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import org.apache.spark.mllib.linalg.{ Vectors, Vector, Matrix }
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
 * Object for calculating correlation and the correlation matrix
 */

object Correlation extends Serializable {

  /**
   * Compute correlation for exactly two columns
   *
   * @param frameRDD input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the correlation
   * @return correlation wrapped in DoubleValue
   */
  def correlation(frameRDD: FrameRDD,
                  dataColumnNames: List[String]): DoubleValue = {
    // compute correlation

    val col1RDD = frameRDD.mapRows(row => {
      DataTypes.toDouble(row.value(dataColumnNames(0)))
    })

    val col2RDD = frameRDD.mapRows(row => {
      DataTypes.toDouble(row.value(dataColumnNames(1)))
    })

    DoubleValue(Statistics.corr(col1RDD, col2RDD))
  }

  /**
   * Compute correlation for two or more columns
   *
   * @param frameRDD input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the correlation matrix
   * @return the correlation matrix in a RDD[Rows]
   */
  def correlationMatrix(frameRDD: FrameRDD,
                        dataColumnNames: List[String]): RDD[sql.Row] = {

    val vectorRDD = frameRDD.mapRows(row => {
      val array = row.valuesAsArray(dataColumnNames)
      val b = array.map(i => DataTypes.toDouble(i))
      Vectors.dense(b)
    })

    val correlation: Matrix = Statistics.corr(vectorRDD)
    val vecArray = correlation.toArray.grouped(correlation.numCols).toArray
    val arrGenericRow = vecArray.map(row => {
      val temp: Array[Any] = row.map(x => x)
      new GenericRow(temp)
    })

    frameRDD.sparkContext.parallelize(arrGenericRow)
  }
}

