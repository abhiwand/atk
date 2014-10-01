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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.descriptives

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ ColumnSummaryStatistics, ColumnSummaryStatisticsReturn }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate summary statistics of the specified column.
 */
class ColumnSummaryStatisticsPlugin extends SparkCommandPlugin[ColumnSummaryStatistics, ColumnSummaryStatisticsReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "dataframe/column_summary_statistics"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Calculate summary statistics of a column.",
    extendedSummary = Some("""
             |        Calculate summary statistics of a column.
             |
             |        Parameters
             |        ----------
             |        data_column : str
             |            The column to be statistically summarized.
             |            Must contain numerical data; all NaNs and infinite values are excluded from the calculation.
             |        weights_column_name : str (optional)
             |            Name of column holding weights of column values
             |        use_population_variance : bool (optional)
             |            If true, the variance is calculated as the population variance. If false, the variance calculated as the
             |             sample variance. Because this option affects the variance, it affects the standard deviation and the
             |             confidence intervals as well. This option is False by default, so that sample variance is the default
             |             form of variance calculated.
             |        Returns
             |        -------
             |        summary : Dict
             |            Dictionary containing summary statistics in the following entries:
             |
             |            | mean:
             |                  Arithmetic mean of the data.
             |
             |            | geometric_mean:
             |                  Geometric mean of the data. None when there is a data element <= 0, 1.0 when there are no data
             |                  elements.
             |
             |            | variance:
             |                  None when there are <= 1 many data elements.
             |                  Sample variance is the weighted sum of the squared distance of each data element from the
             |                   weighted mean, divided by the total weight minus 1. None when the sum of the weights is <= 1.
             |                  Population variance is the weighted sum of the squared distance of each data element from the
             |                   weighted mean, divided by the total weight.
             |
             |            | standard_deviation:
             |                  The square root of the variance. None when  sample variance
             |                  is being used and the sum of weights is <= 1.
             |
             |
             |            | valid_data_count:
             |                  The count of all data elements that are finite numbers.
             |                  (In other words, after excluding NaNs and infinite values.)
             |
             |            | minimum:
             |                  Minimum value in the data. None when there are no data elements.
             |
             |            | maximum:
             |                  Maximum value in the data. None when there are no data elements.
             |
             |            | mean_confidence_lower:
             |                  Lower limit of the 95% confidence interval about the mean.
             |                  Assumes a Gaussian distribution.
             |                  None when there are no elements of positive weight.
             |
             |            | mean_confidence_upper:
             |                  Upper limit of the 95% confidence interval about the mean.
             |                  Assumes a Gaussian distribution.
             |                  None when there are no elements of positive weight.
             |
             |            | bad_row_count : The number of rows containing a NaN or infinite value in either the data
             |                  or weights column.
             |
             |            | good_row_count : The number of rows not containing a NaN or infinite value in either the data
             |                  or weights column.
             |
             |            | positive_weight_count : The number of valid data elements with weight > 0.
             |                   This is the number of entries used in the statistical calculation.
             |
             |            | non_positive_weight_count : The number valid data elements with finite weight <= 0.
             |
             |        Notes
             |        -----
             |        Return Types
             |            | valid_data_count returns a Long.
             |            | All other values are returned as Doubles or None.
             |
             |        Sample Variance
             |            Sample Variance is computed by the following formula:
             |
             |        .. math::
             |
             |            \\left( \\frac{1}{W - 1} \\right) * sum_{i}  \\left(x_{i} - M \\right) ^{2}
             |
             |        where :math:`W` is sum of weights over valid elements of positive weight, and :math:`M` is the weighted mean.
             |
             |        Population Variance
             |            Population Variance is computed by the following formula:
             |
             |        .. math::
             |
             |            \\left( \\frac{1}{W} \\right) * sum_{i}  \\left(x_{i} - M \\right) ^{2}
             |
             |        where :math:`W` is sum of weights over valid elements of positive weight, and :math:`M` is the weighted mean.
             |
             |        Standard Deviation
             |            The square root of the variance.
             |
             |        Logging Invalid Data
             |        --------------------
             |
             |        A row is bad when it contains a NaN or infinite value in either its data or weights column.  In this case, it
             |         contributes to bad_row_count; otherwise it contributes to good row count.
             |
             |        A good row can be skipped because the value in its weight column is <=0. In this case, it contributes to
             |         non_positive_weight_count, otherwise (when the weight is > 0) it contributes to valid_data_weight_pair_count.
             |
             |            Equations
             |            ---------
             |            bad_row_count + good_row_count = # rows in the frame
             |            positive_weight_count + non_positive_weight_count = good_row_count
             |
             |        In particular, when no weights column is provided and all weights are 1.0, non_positive_weight_count = 0 and
             |         positive_weight_count = good_row_count
             |
             |        Examples
             |        --------
             |        ::
             |
             |            stats = frame.column_summary_statistics('data column', 'weight column')
             |
             |        .. versionadded:: 0.8
             |""".stripMargin)))

  /**
   * Calculate summary statistics of the specified column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments Input specification for column summary statistics.
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: ColumnSummaryStatistics)(implicit user: UserPrincipal, executionContext: ExecutionContext): ColumnSummaryStatisticsReturn = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val frameId: Long = arguments.frame.id
    val frame = frames.expectFrame(frameId)
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)
    val valueDataType: DataType = frame.schema.columns(columnIndex)._2
    val usePopulationVariance = arguments.usePopulationVariance.getOrElse(false)
    val (weightsColumnIndexOption, weightsDataTypeOption) = if (arguments.weightsColumn.isEmpty) {
      (None, None)
    }
    else {
      val weightsColumnIndex = frame.schema.columnIndex(arguments.weightsColumn.get)
      (Some(weightsColumnIndex), Some(frame.schema.columns(weightsColumnIndex)._2))
    }

    // run the operation and return the results
    val rdd = frames.loadFrameRdd(ctx, frameId)
    ColumnStatistics.columnSummaryStatistics(columnIndex,
      valueDataType,
      weightsColumnIndexOption,
      weightsDataTypeOption,
      rdd,
      usePopulationVariance)
  }
}
