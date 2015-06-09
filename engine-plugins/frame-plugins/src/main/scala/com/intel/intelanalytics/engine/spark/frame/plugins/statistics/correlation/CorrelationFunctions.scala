/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.correlation

import breeze.linalg.DenseVector
import breeze.numerics._
import com.intel.intelanalytics.domain.DoubleValue
import com.intel.intelanalytics.domain.DoubleValue
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes
import org.apache.spark.mllib.linalg.{ Vectors, Vector, Matrix }
import org.apache.spark.frame.FrameRdd
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
   * @param frameRdd input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the correlation
   * @return correlation wrapped in DoubleValue
   */
  def correlation(frameRdd: FrameRdd,
                  dataColumnNames: List[String]): DoubleValue = {
    // compute correlation

    val correlation: Matrix = Statistics.corr(frameRdd.toVectorDenseRDD(dataColumnNames))

    val dblVal: Double = correlation.toArray(1)

    DoubleValue(if (dblVal.isNaN || abs(dblVal) < .000001) 0 else dblVal)
  }

  /**
   * Compute correlation for two or more columns
   *
   * @param frameRdd input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the correlation matrix
   * @return the correlation matrix in a RDD[Rows]
   */
  def correlationMatrix(frameRdd: FrameRdd,
                        dataColumnNames: List[String]): RDD[sql.Row] = {

    val correlation: Matrix = Statistics.corr(frameRdd.toVectorDenseRDD(dataColumnNames))
    val vecArray = correlation.toArray.grouped(correlation.numCols).toArray
    val arrGenericRow = vecArray.map(row => {
      val temp: Array[Any] = row.map(x => if (x.isNaN || abs(x) < .000001) 0 else x)
      new GenericRow(temp)
    })

    frameRdd.sparkContext.parallelize(arrGenericRow)
  }
}
