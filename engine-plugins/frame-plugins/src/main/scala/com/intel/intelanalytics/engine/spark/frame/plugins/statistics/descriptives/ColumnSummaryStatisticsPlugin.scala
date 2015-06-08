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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.descriptives

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameReference, ColumnSummaryStatisticsArgs, ColumnSummaryStatisticsReturn }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate summary statistics of the specified column.
 * Parameters
 * ----------
 * data_column : str
 *   The column to be statistically summarized.
 *   Must contain numerical data; all NaNs and infinite values are excluded
 *   from the calculation.
 * weights_column_name : str (optional)
 *   Name of column holding weights of column values.
 * use_population_variance : bool (optional)
 *   If true, the variance is calculated as the population variance.
 *   If false, the variance calculated as the sample variance.
 *   Because this option affects the variance, it affects the standard
 *   deviation and the confidence intervals as well.
 *   This option is False by default, so that sample variance is the default
 *   form of variance calculated.
 */
@PluginDoc(oneLine = "Calculate multiple statistics for a column.",
  extended = """Notes
-----
Sample Variance
    Sample Variance is computed by the following formula:

    .. math::

        \left( \frac{1}{W - 1} \right) * sum_{i} \
        \left(x_{i} - M \right) ^{2}

    where :math:`W` is sum of weights over valid elements of positive
    weight, and :math:`M` is the weighted mean.

Population Variance
    Population Variance is computed by the following formula:

    .. math::

        \left( \frac{1}{W} \right) * sum_{i} \
        \left(x_{i} - M \right) ^{2}

    where :math:`W` is sum of weights over valid elements of positive
    weight, and :math:`M` is the weighted mean.

Standard Deviation
    The square root of the variance.

Logging Invalid Data
    A row is bad when it contains a NaN or infinite value in either
    its data or weights column.
    In this case, it contributes to bad_row_count; otherwise it
    contributes to good row count.

    A good row can be skipped because the value in its weight
    column is less than or equal to 0.
    In this case, it contributes to non_positive_weight_count, otherwise
    (when the weight is greater than 0) it contributes to
    valid_data_weight_pair_count.

Equations
    ``bad_row_count + good_row_count = # rows in the frame``

    ``positive_weight_count + non_positive_weight_count = good_row_count``

    In particular, when no weights column is provided and all weights
    are 1.0, ``non_positive_weight_count = 0`` and
    ``positive_weight_count = good_row_count``

""",
  returns = """dict
    Dictionary containing summary statistics.
    The data returned is composed of multiple components:
mean : [ double | None ]
    Arithmetic mean of the data.
geometric_mean : [ double | None ]
    Geometric mean of the data.
    None when there is a data element <= 0, 1.0 when there are no
    data elements.
variance : [ double | None ]
    None when there are <= 1 many data elements.
    Sample variance is the weighted sum of the squared distance of
    each data element from the weighted mean, divided by the total
    weight minus 1.
    None when the sum of the weights is <= 1.
    Population variance is the weighted sum of the squared distance
    of each data element from the weighted mean, divided by the total
    weight.
standard_deviation : [ double | None ]
    The square root of the variance. None when  sample variance
    is being used and the sum of weights is <= 1.
valid_data_count : long
    The count of all data elements that are finite numbers.
    (In other words, after excluding NaNs and infinite values.)
minimum : [ double | None ]
    Minimum value in the data.
    None when there are no data elements.
maximum : [ double | None ]
    Maximum value in the data. None when there are no data elements.
mean_confidence_lower : [ double | None ]
    Lower limit of the 95% confidence interval about the mean.
    Assumes a Gaussian distribution.
    None when there are no elements of positive weight.
mean_confidence_upper : [ double | None ]
    Upper limit of the 95% confidence interval about the mean.
    Assumes a Gaussian distribution.
    None when there are no elements of positive weight.
bad_row_count : [ double | None ]
    The number of rows containing a NaN or infinite value
    in either the data or weights column.
good_row_count : [ double | None ]
    The number of rows not containing a NaN or infinite
    value in either the data or weights column.
positive_weight_count : [ double | None ]
    The number of valid data elements with weight > 0.
    This is the number of entries used in the statistical calculation.
non_positive_weight_count : [ double | None ]
    The number valid data elements with finite weight <= 0.

""")
class ColumnSummaryStatisticsPlugin extends SparkCommandPlugin[ColumnSummaryStatisticsArgs, ColumnSummaryStatisticsReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/column_summary_statistics"

  /**
   * Calculate summary statistics of the specified column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments Input specification for column summary statistics.
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ColumnSummaryStatisticsArgs)(implicit invocation: Invocation): ColumnSummaryStatisticsReturn = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    // validate arguments
    val frameRef: FrameReference = arguments.frame
    val frame = frames.expectFrame(frameRef)
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)
    val valueDataType: DataType = frame.schema.columnTuples(columnIndex)._2
    val usePopulationVariance = arguments.usePopulationVariance.getOrElse(false)
    val (weightsColumnIndexOption, weightsDataTypeOption) = if (arguments.weightsColumn.isEmpty) {
      (None, None)
    }
    else {
      val weightsColumnIndex = frame.schema.columnIndex(arguments.weightsColumn.get)
      (Some(weightsColumnIndex), Some(frame.schema.columnTuples(weightsColumnIndex)._2))
    }

    // run the operation and return the results
    val rdd = frames.loadLegacyFrameRdd(sc, frameRef)
    ColumnStatistics.columnSummaryStatistics(columnIndex,
      valueDataType,
      weightsColumnIndexOption,
      weightsDataTypeOption,
      rdd,
      usePopulationVariance)
  }
}
