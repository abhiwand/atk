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
 * Command for calculating summary statistics for a dataframe column.
 * @param frame Identifier for the input dataframe.
 * @param dataColumn Name of the column to statistically summarize. Must contain numerical data.
 * @param weightsColumn Optional. Name of the column that provides weights (frequencies).
 */
case class ColumnSummaryStatistics(frame: FrameReference, dataColumn: String, weightsColumn: Option[String]) {

  require(frame != null, "frame is required but not provided")
  require(dataColumn != null, "data column is required but not provided")
}

/**
 * Summary statistics for a dataframe column. All values are optionally weighted by a second column. If no weights are
 * provided, all elements receive a uniform weight of 1. If any element receives a weight <= 0, that element is thrown
 * out of the calculation. If a row contains a NaN or infinite value in either the data or weights column, that row is
 * skipped and a count of bad rows is incremented.
 *
 * Values follow default settings specified by SAS
 * http://support.sas.com/documentation/cdl/en/procstat/63104/HTML/default/viewer.htm#procstat_univariate_sect026.htm
 * @param mean Arithmetic mean of the data.
 * @param geometricMean Geometric mean of the data. NaN when there is a non-positive data element, 1 if there are no
 *                       data elements.
 * @param variance Variance of the data where weighted sum of squared distance from the mean is divided by the number of
 *                 data elements minus 1. NaN when the number of data elements is < 2.
 * @param standardDeviation Standard deviation of the data. NaN when the number of data elements is < 2.
 * @param mode A mode of the data; that is, an item with the greatest weight (largest frequency).
 *             Ties resolved arbitrarily. NaN when there are no data elements of positive weight.
 * @param minimum Minimum value in the data. NaN when there are no data elements of positive weight.
 * @param maximum Maximum value in the data. NaN when there are no data elements of positive weight.
 * @param meanConfidenceLowerBound: Lower limit of the 95% confidence interval about the mean. Assumes a Gaussian RV.
 *                             NaN when there are <= 1 data elements of positive weight.
 * @param meanConfidenceUpperBound: Upper limit of the 95% confidence interval about the mean. Assumes a Gaussian RV.
 *                              NaN when there are <= 1 data elements of positive weight.
 * @param positiveWeightCount  The number data elements with weights > 0.
 * @param nonPositiveWeightCount The number data elements with weight <= 0.
 * @param badRowCount The number of rows containing a NaN or infinite value in either the data or weights column.
 * @param validDataWeightPairCount The number of rows containing a NaN or infinite value in either the data or weights
 *                             column. This is the number of entries used for the calculation of the statistics.
 */
case class ColumnSummaryStatisticsReturn(mean: Double,
                                         geometricMean: Double,
                                         variance: Double,
                                         standardDeviation: Double,
                                         mode: Double,
                                         minimum: Double,
                                         maximum: Double,
                                         meanConfidenceLowerBound: Double,
                                         meanConfidenceUpperBound: Double,
                                         positiveWeightCount: Long,
                                         nonPositiveWeightCount: Long,
                                         badRowCount: Long,
                                         validDataWeightPairCount: Long)

