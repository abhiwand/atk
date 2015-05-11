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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.numericalstatistics

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.frame.ColumnFullStatisticsReturn

/**
 * Statistics calculator for weighted numerical data. Data elements with non-positive weights are thrown out and do
 * not affect stastics (excepting the count of entries with non-postive weights).
 *
 * @param dataWeightPairs RDD of pairs of  the form (data, weight)
 */
class NumericalStatistics(dataWeightPairs: RDD[(Option[Double], Option[Double])], usePopulationVariance: Boolean) extends Serializable {

  /*
   * Incoming weights and data are Doubles, but internal running sums are represented as BigDecimal to improve
   * numerical stability.
   *
   * Values are recast to Doubles before being returned because we do not want to give the false impression that
   * we are improving precision. The use of BigDecimals is only to reduce accumulated rounding error while combing
   * values over many, many entries.
   */

  private lazy val singlePassStatistics: FirstPassStatistics = StatisticsRddFunctions.generateFirstPassStatistics(dataWeightPairs)

  /**
   * The weighted mean of the data.
   */
  lazy val weightedMean: Double = singlePassStatistics.mean.toDouble

  /**
   * The weighted geometric mean of the data. NaN when a data element is <= 0,
   * 1 when there are no data elements of positive weight.
   */
  lazy val weightedGeometricMean: Double = {

    val totalWeight: BigDecimal = singlePassStatistics.totalWeight
    val weightedSumOfLogs: Option[BigDecimal] = singlePassStatistics.weightedSumOfLogs

    if (totalWeight > 0 && weightedSumOfLogs.nonEmpty)
      Math.exp((weightedSumOfLogs.get / totalWeight).toDouble)
    else if (totalWeight > 0 && weightedSumOfLogs.isEmpty) {
      Double.NaN
    }
    else {
      // this is the totalWeight == 0 case
      1.toDouble
    }
  }

  /**
   * The weighted variance of the data. NaN when there are <=1 data elements.
   */
  lazy val weightedVariance: Double = {
    val weight: BigDecimal = singlePassStatistics.totalWeight
    if (usePopulationVariance) {
      (singlePassStatistics.weightedSumOfSquaredDistancesFromMean / weight).toDouble
    }
    else {
      if (weight > 1)
        (singlePassStatistics.weightedSumOfSquaredDistancesFromMean / (weight - 1)).toDouble
      else
        Double.NaN
    }
  }

  /**
   * The weighted standard deviation of the data. NaN when there are <=1 data elements of nonzero weight.
   */
  lazy val weightedStandardDeviation: Double = Math.sqrt(weightedVariance)

  /**
   * Sum of all weights that are finite numbers  > 0.
   */
  lazy val totalWeight: Double = singlePassStatistics.totalWeight.toDouble

  /**
   * The minimum value of the data. Positive infinity when there are no data elements of positive weight.
   */
  lazy val min: Double = if (singlePassStatistics.minimum.isInfinity) Double.NaN else singlePassStatistics.minimum

  /**
   * The maximum value of the data. Negative infinity when there are no data elements of positive weight.
   */
  lazy val max: Double = if (singlePassStatistics.maximum.isInfinity) Double.NaN else singlePassStatistics.maximum

  /**
   * The number of elements in the data set with weight > 0.
   */
  lazy val positiveWeightCount: Long = singlePassStatistics.positiveWeightCount

  /**
   * The number of pairs that contained NaNs or infinite values for a data column or a weight column (if the weight column
   */
  lazy val badRowCount: Long = singlePassStatistics.badRowCount

  /**
   * The number of pairs that contained proper finite numbers for the data column and the weight column.
   */
  lazy val goodRowCount: Long = singlePassStatistics.goodRowCount

  /**
   * The number of elements in the data set with weight <= 0.
   */
  lazy val nonPositiveWeightCount: Long = singlePassStatistics.nonPositiveWeightCount

  /**
   * The lower limit of the 95% confidence interval about the mean. (Assumes that the distribution is normal.)
   * NaN when the total weight is 0.
   */
  lazy val meanConfidenceLower: Double =

    if (positiveWeightCount > 1 && weightedStandardDeviation != Double.NaN)
      weightedMean - (1.96) * (weightedStandardDeviation / Math.sqrt(totalWeight))
    else
      Double.NaN

  /**
   * The lower limit of the 95% confidence interval about the mean. (Assumes that the distribution is normal.)
   * NaN when the total weight is 0.
   */
  lazy val meanConfidenceUpper: Double =
    if (totalWeight > 0 && weightedStandardDeviation != Double.NaN)
      weightedMean + (1.96) * (weightedStandardDeviation / Math.sqrt(totalWeight))
    else
      Double.NaN

}