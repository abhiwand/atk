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

package com.intel.intelanalytics.engine.spark.frame.plugins.dotproduct

import java.io.Serializable

import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.spark.frame.RowWrapper
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
 * Object for calculating dot product
 */
object DotProductFunctions extends Serializable {

  /**
   * Computes the dot product for each row in the frame.
   *
   * Uses the left column values, and the right column values to compute the dot product for each row.
   *
   * @param frameRdd Data frame
   * @param leftColumnsNames Left column names
   * @param rightColumnsNames Right column names
   * @param defaultLeftValues  Optional default values used to substitute null values in the left columns
   * @param defaultRightValues Optional default values used to substitute null values in the right columns
   * @return Data frame with an additional column containing the dot product
   */
  def dotProduct(frameRdd: FrameRdd, leftColumnsNames: List[String], rightColumnsNames: List[String],
                 defaultLeftValues: Option[List[Double]] = None,
                 defaultRightValues: Option[List[Double]] = None): RDD[sql.Row] = {

    frameRdd.mapRows(row => {
      val leftVector = createVector(row, leftColumnsNames, defaultLeftValues)
      val rightVector = createVector(row, rightColumnsNames, defaultRightValues)
      val dotProduct = computeDotProduct(leftVector, rightVector)
      new GenericRow(row.valuesAsArray() :+ dotProduct)
    })
  }

  /**
   * Create vector of doubles from column values.
   *
   * The columns contain numeric data represented as scalars or vectors
   *
   * @param row Input row
   * @param columnNames Input column names
   * @param defaultValues Default values used to substitute nulls
   * @return Vector of doubles
   */
  def createVector(row: RowWrapper, columnNames: List[String], defaultValues: Option[List[Double]]): Vector[Double] = {
    val vector = row.valuesAsArray(columnNames, flattenInputs = true)
    replaceNaNs(DataTypes.toVector(vector), defaultValues)
  }

  /**
   * Replace NaNs in input vector with default values
   *
   * @param vector Input vector
   * @param defaultValues Default values
   * @return Vector with NaNs replaced by defaults or zero
   */
  def replaceNaNs(vector: Vector[Double], defaultValues: Option[List[Double]]): Vector[Double] = {
    require(defaultValues.isEmpty || defaultValues.get.size == vector.size, s"size in default values should be ${vector.size}")

    vector.zipWithIndex.map {
      case (value, i) =>
        value match {
          case x if x.isNaN => if (defaultValues.isDefined) defaultValues.get(i) else 0d
          case _ => value
        }
    }
  }

  /**
   * Computes the dot product of two vectors
   *
   * The dot product is the sum of the products of the corresponding entries in the two vectors.
   *
   * @param leftVector Left vector
   * @param rightVector Right vector
   * @return Dot product
   */
  def computeDotProduct(leftVector: Vector[Double], rightVector: Vector[Double]): Double = {
    require(!leftVector.isEmpty, "left vector should not be empty")
    require(leftVector.size == rightVector.size, "size of left vector should equal size of right vector")

    var dotProduct = 0d
    for (i <- 0 until leftVector.size) {
      dotProduct += leftVector(i) * rightVector(i)
    }
    dotProduct
  }

}
