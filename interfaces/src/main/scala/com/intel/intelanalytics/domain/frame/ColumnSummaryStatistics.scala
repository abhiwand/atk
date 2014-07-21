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

package com.intel.intelanalytics.domain.frame

/**
 * Command specification for calculating summary statistics for a dataframe column.
 * @param frame Identifier for the input dataframe.
 * @param data_column Name of the column to statistically summarize. Must contain numerical data.
 * @param weights_column Optional. Name of the column that provides weights (frequencies).
 */
case class ColumnSummaryStatistics(frame: FrameReference, data_column: String, weights_column: Option[String]) {

  require(frame != null, "frame is required but not provided")
  require(data_column != null, "data_column is required but not provided")
}

/**
 * Summary statistics for a dataframe column. All values are optionally weighted by a second column. If no weights are
 * provided, all elements receive a uniform weight of 1. If any element receives a weight <= 0, that element is thrown
 * out of the calculation.
 *
 * Values follow default settings specified by SAS
 * http://support.sas.com/documentation/cdl/en/procstat/63104/HTML/default/viewer.htm#procstat_univariate_sect026.htm
 * @param mean Arithmetic mean of the data.
 * @param geometric_mean Geometric mean of the data. NaN when there is a non-positive data element, 1 if there are no
 *                       data elements.
 * @param variance Variance of the data where weighted sum of squared distance from the mean is divided by the number of
 *                 data elements minus 1. NaN when the number of data elements is < 2.
 * @param standard_deviation Standard deviation of the data. NaN when the number of data elements is < 2.
 * @param mode A mode of the data; that is, an item with the greatest weight (largest frequency).
 *             Ties resolved arbitrarily. NaN when there are no data elements of positive weight.
 * @param minimum Minimum value in the data. Positive infinity when there are no data elements of positive
 * weight.
 * @param maximum Maximum value in the data. Negative infinity when there are no data elements of positive
 * weight.
 * @param mean_confidence_lower: Lower limit of the 95% confidence interval about the mean. Assumes a Gaussian RV.
 *                             NaN when there are <= 1 data elements of positive weight.
 * @param mean_confidence_upper: Upper limit of the 95% confidence interval about the mean. Assumes a Gaussian RV.
 *                              NaN when there are <= 1 data elements of positive weight.
 * @param count The number data elements. Equivalently, the count of rows in the column.
 * @param non_positive_weight_count The number data elements with weight <= 0.
 */
case class ColumnSummaryStatisticsReturn(mean: Double,
                                         geometric_mean: Double,
                                         variance: Double,
                                         standard_deviation: Double,
                                         mode: Double,
                                         minimum: Double,
                                         maximum: Double,
                                         mean_confidence_lower: Double,
                                         mean_confidence_upper: Double,
                                         count: Long,
                                         non_positive_weight_count: Long)

