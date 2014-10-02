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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ ColumnMedianReturn, ColumnMedian, DataFrame }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.engine.spark.statistics.ColumnStatistics
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate the median of the specified column.
 */
class ColumnMedianPlugin extends SparkCommandPlugin[ColumnMedian, ColumnMedianReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "dataframe/column_median"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Calculate (weighted) median of a column.",
    extendedSummary = Some("""
   |Calculate the (weighted) median of a column. The median is the least value X in the range of the distribution so
   |         that the cumulative weight of values strictly below X is strictly less than half of the total weight and
   |          the cumulative weight of values up to and including X is >= 1/2 the total weight.
   |
   |        All data elements of weight <= 0 are excluded from the calculation, as are all data elements whose weight
   |         is NaN or infinite. If a weight column is provided and no weights are finite numbers > 0, None is returned.
   |
   |        Parameters
   |        ----------
   |        data_column : str
   |            The column whose median is to be calculated.
   |
   |        weights_column : str
   |            Optional. The column that provides weights (frequencies) for the median calculation.
   |            Must contain numerical data. Uniform weights of 1 for all items will be used for the calculation if this
   |                parameter is not provided.
   |
   |        Returns
   |        -------
   |        median :  The median of the values.  If a weight column is provided and no weights are finite numbers > 0,
   |             None is returned. Type of the median returned is that of the contents of the data column, so a column of
   |             Longs will result in a Long median and a column of Floats will result in a Float median.
   |
   |        Example
   |        -------
   |        >>> median = frame.column_median('middling column')
   |""".stripMargin)))

  /**
   * Calculate the median of the specified column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments Input specification for column median.
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: ColumnMedian)(implicit user: UserPrincipal, executionContext: ExecutionContext): ColumnMedianReturn = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val frameId: Long = arguments.frame.id
    val frame = frames.expectFrame(frameId)
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)
    val valueDataType: DataType = frame.schema.columns(columnIndex)._2

    // run the operation and return results
    val rdd = frames.loadFrameData(ctx, frameId)
    val (weightsColumnIndexOption, weightsDataTypeOption) = if (arguments.weightsColumn.isEmpty) {
      (None, None)
    }
    else {
      val weightsColumnIndex = frame.schema.columnIndex(arguments.weightsColumn.get)
      (Some(weightsColumnIndex), Some(frame.schema.columns(weightsColumnIndex)._2))
    }
    ColumnStatistics.columnMedian(columnIndex, valueDataType, weightsColumnIndexOption, weightsDataTypeOption, rdd)
  }
}
