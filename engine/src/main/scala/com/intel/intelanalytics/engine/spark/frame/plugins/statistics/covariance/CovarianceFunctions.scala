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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.covariance

import breeze.linalg.DenseVector
import breeze.numerics.abs
import com.intel.intelanalytics.domain.DoubleValue
import com.intel.intelanalytics.domain.schema.DataTypes
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.{ Vectors, Vector, Matrix }
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
 * Object for calculating covariance and the covariance matrix
 */

object CovarianceFunctions extends Serializable {

  /**
   * Compute covariance for exactly two columns
   *
   * @param frameRdd input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the covariance
   * @return covariance wrapped in CovarianceReturn
   */
  def covariance(frameRdd: FrameRdd,
                 dataColumnNames: List[String]): DoubleValue = {

    // compute and return covariance
    def rowMatrix: RowMatrix = new RowMatrix(frameRdd.toVectorDenseRDD(dataColumnNames))

    val covariance: Matrix = rowMatrix.computeCovariance()

    val dblVal: Double = covariance.toArray(1)

    DoubleValue(if (dblVal.isNaN || abs(dblVal) < .000001) 0 else dblVal)
  }

  /**
   * Compute covariance for two or more columns
   *
   * @param frameRdd input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the covariance matrix
   * @param outputVectorLength If specified, output results as a column of type 'vector(vectorOutputLength)'
   * @return the covariance matrix in a RDD[Rows]
   */
  def covarianceMatrix(frameRdd: FrameRdd,
                       dataColumnNames: List[String],
                       outputVectorLength: Option[Long] = None): RDD[sql.Row] = {

    def rowMatrix: RowMatrix = new RowMatrix(frameRdd.toVectorDenseRDD(dataColumnNames))

    val covariance: Matrix = rowMatrix.computeCovariance()
    val vecArray = covariance.toArray.grouped(covariance.numCols).toArray
    val formatter: Array[Any] => Array[Any] = outputVectorLength match {
      case Some(length) =>
        val vectorizer = DataTypes.toVector(length)_
        x => Array(vectorizer(x))
        case _ => identity
    }

    val arrGenericRow = vecArray.map(row => {
      val formattedRow: Array[Any] = formatter(row.map(x => if (x.isNaN || abs(x) < .000001) 0 else x))
      new GenericRow(formattedRow)
    })

    frameRdd.sparkContext.parallelize(arrGenericRow)
  }
}
