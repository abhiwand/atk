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

package com.intel.intelanalytics.engine.spark.frame.plugins.groupby.aggregators

import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.spark.frame.plugins.bincolumn.DiscretizationFunctions

/**
 * Aggregator for computing histograms using a list of cutoffs.
 *
 * The histogram is a vector containing the percentage of observations found in each bin
 */
case class HistogramAggregator(cutoffs: List[Double], includeLowest: Option[Boolean] = None, strictBinning: Option[Boolean] = None) extends GroupByAggregator {
  require(cutoffs.length > 1, "Length of cutoffs should be greater than 1")

  /** An array that aggregates the number of elements in each bin */
  override type AggregateType = Array[Double]

  /** The bin number for a column value */
  override type ValueType = Int

  /** The 'empty' or 'zero' or default value for the aggregator */
  override def zero = Array.ofDim[Double](cutoffs.size - 1)

  /**
   * Get the bin index for the column value based on the cutoffs
   *
   * Strict binning is disabled so values smaller than the first bin are assigned to the first bin,
   * and values larger than the last bin are assigned to the last bin.
   */
  override def mapFunction(columnValue: Any, columnDataType: DataType): ValueType = {
    if (columnValue != null) {
      DiscretizationFunctions.binElement(DataTypes.toDouble(columnValue),
        cutoffs,
        lowerInclusive = includeLowest.getOrElse(true),
        strictBinning = strictBinning.getOrElse(false))
    }
    else -1
  }

  /**
   * Increment the count for the bin corresponding to the bin index
   */
  override def add(binArray: AggregateType, binIndex: ValueType): AggregateType = {
    if (binIndex >= 0) binArray(binIndex) += 1
    binArray
  }

  /**
   * Sum two binned lists.
   */
  override def merge(binArray1: AggregateType, binArray2: AggregateType) = {
    (binArray1, binArray2).zipped.map(_ + _)
  }

  /**
   * Return the vector containing the percentage of observations found in each bin
   */
  override def getResult(binArray: AggregateType): Any = {
    val total = binArray.sum
    if (total > 0) DataTypes.toVector(binArray.map(_ / total)) else binArray
  }
}
