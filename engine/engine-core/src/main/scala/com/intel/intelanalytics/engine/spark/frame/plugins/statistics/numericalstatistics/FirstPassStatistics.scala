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
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.NumericValidationUtils

/**
 * Contains all statistics that are computed in a single pass over the data. All statistics are in their weighted form.
 *
 * Floating point values that are running combinations over all of the data are represented as BigDecimal, whereas
 * minimum, mode and maximum are Doubles since they are simply single data points.
 *
 * Data values that are NaNs or infinite or whose weights are Nans or infinite or <=0 are skipped and logged.
 *
 * @param mean The weighted mean of the data.
 * @param weightedSumOfSquares Weighted mean of the data values squared.
 * @param weightedSumOfSquaredDistancesFromMean Weighted sum of squared distances from the weighted mean.
 * @param weightedSumOfLogs Weighted sum of logarithms of the data.
 * @param minimum The minimum data value of finite weight > 0.
 * @param maximum The minimum data value of finite weight > 0.
 * @param totalWeight The total weight in the column, excepting data pairs whose data is not a finite number, or whose
 *                    weight is either not a finite number or <= 0.
 * @param positiveWeightCount Number of entries whose weight is a finite number > 0.
 * @param nonPositiveWeightCount Number of entries whose weight is a finite number <= 0.
 * @param badRowCount The number of entries that contain a data value or a weight that is a not a finite number.
 * @param goodRowCount The number of entries that whose data value and weight are both finite numbers.
 */
private[numericalstatistics] case class FirstPassStatistics(mean: BigDecimal,
                                                            weightedSumOfSquares: BigDecimal,
                                                            weightedSumOfSquaredDistancesFromMean: BigDecimal,
                                                            weightedSumOfLogs: Option[BigDecimal],
                                                            minimum: Double,
                                                            maximum: Double,
                                                            totalWeight: BigDecimal,
                                                            positiveWeightCount: Long,
                                                            nonPositiveWeightCount: Long,
                                                            badRowCount: Long,
                                                            goodRowCount: Long)
