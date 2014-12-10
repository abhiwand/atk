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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.covariance

import breeze.linalg.DenseVector
import com.intel.intelanalytics.domain.frame.CovarianceReturn
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import org.apache.spark.mllib.linalg.{ Vectors, Vector, Matrix }
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
 * Object for calculating covariance and the covariance matrix
 */

object Covariance extends Serializable {

  /**
   * Compute covariance for exactly two columns
   *
   * @param frameRDD input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the covariance
   * @return covariance wrapped in CovarianceReturn
   */
  def covariance(frameRDD: FrameRDD,
                 dataColumnNames: List[String]): CovarianceReturn = {
    // compute multivariate statistics and return covariance

    val rowsAsVectorRDD = frameRDD.mapRows(row => {
      val array = row.valuesAsArray(dataColumnNames)
      val b = array.map(i => DataTypes.toDouble(i))
      Vectors.dense(b)
    })

    def rowMatrix: RowMatrix = new RowMatrix(rowsAsVectorRDD)

    val (rowCount, mean) = rowMatrix.rows.aggregate[(Long, DenseVector[Double])]((0L, DenseVector.zeros[Double](rowsAsVectorRDD.first().size)))(
      seqOp = (s: (Long, DenseVector[Double]), v: Vector) => (s._1 + 1L, s._2 += DenseVector(v.toArray)),
      combOp = (s1: (Long, DenseVector[Double]), s2: (Long, DenseVector[Double])) =>
        (s1._1 + s2._1, s1._2 += s2._2)
    )
    mean :/= rowCount.toDouble

    val product = rowMatrix.rows.aggregate[Double](0)((s: Double, v: Vector) => {
      val d = v.toArray
      d(0) * d(1)
    }, combOp = (s1: Double, s2: Double) => (s1 + s2))

    val covariance = (product / (rowCount - 1)) - (mean(0) * mean(1) * rowCount / (rowCount - 1))
    CovarianceReturn(covariance)
  }

  /**
   * Compute covariance for two or more columns
   *
   * @param frameRDD input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the covariance matrix
   * @return the covariance matrix in a RDD[Rows]
   */
  def covarianceMatrix(frameRDD: FrameRDD,
                       dataColumnNames: List[String]): RDD[sql.Row] = {

    val vectorRDD = frameRDD.mapRows(row => {
      val array = row.valuesAsArray(dataColumnNames)
      val b = array.map(i => DataTypes.toDouble(i))
      Vectors.dense(b)
    })

    def rowMatrix: RowMatrix = new RowMatrix(vectorRDD)

    val covariance: Matrix = rowMatrix.computeCovariance()
    val vecArray = covariance.toArray.grouped(covariance.numCols).toArray
    val arrGenericRow = vecArray.map(row => {
      val temp: Array[Any] = row.map(x => x)
      new GenericRow(temp)
    })

    frameRDD.sparkContext.parallelize(arrGenericRow)
  }
}

