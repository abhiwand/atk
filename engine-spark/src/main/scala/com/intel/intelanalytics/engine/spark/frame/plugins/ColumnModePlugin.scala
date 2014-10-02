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
import com.intel.intelanalytics.domain.frame.{ ColumnModeReturn, ColumnMode, DataFrame }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.engine.spark.statistics.ColumnStatistics
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate modes of a column.
 */
class ColumnModePlugin extends SparkCommandPlugin[ColumnMode, ColumnModeReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "dataframe/column_mode"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Calculate modes of a column.",
    extendedSummary = Some("""
    Calculate modes of a column.  A mode is a data element of maximum weight. All data elements of weight <= 0
    are excluded from the calculation, as are all data elements whose weight is NaN or infinite.
    If there are no data elements of finite weight > 0, no mode is returned.

    Because data distributions often have mutliple modes, it is possible for a set of modes to be returned. By
    default, only one is returned, but my setting the optional parameter max_number_of_modes_returned, a larger
    number of modes can be returned.

    Parameters
    ----------
    data_column : str
        The column whose mode is to be calculated

    weights_column : str
        Optional. The column that provides weights (frequencies) for the mode calculation.
        Must contain numerical data. Uniform weights of 1 for all items will be used for the calculation if this
        parameter is not provided.

    max_modes_returned : int
        Optional. Maximum number of modes returned. If this parameter is not provided, it defaults to 1

    Returns
    -------
    mode : Dict
        Dictionary containing summary statistics in the following entries:
            mode : A mode is a data element of maximum net weight. A set of modes is returned.
             The empty set is returned when the sum of the weights is 0. If the number of modes is <= the parameter
             maxNumberOfModesReturned, then all modes of the data are returned.If the number of modes is
             > maxNumberOfModesReturned, then only the first maxNumberOfModesReturned many modes
             (per a canonical ordering) are returned.
            weight_of_mode : Weight of a mode. If there are no data elements of finite weight > 0,
             the weight of the mode is 0. If no weights column is given, this is the number of appearances of
             each mode.
            total_weight : Sum of all weights in the weight column. This is the row count if no weights
             are given. If no weights column is given, this is the number of rows in the table with non-zero weight.
            mode_count : The number of distinct modes in the data. In the case that the data is very multimodal,
             this number may well exceed max_number_of_modes_returned.

    Example
    -------
    >>> mode = frame.column_mode('modum columpne')
                           """)))

  /**
   * Calculate modes of a column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: ColumnMode)(implicit user: UserPrincipal, executionContext: ExecutionContext): ColumnModeReturn = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val frameId = arguments.frame.id
    val frame = frames.expectFrame(frameId)

    // run the operation and return results
    val rdd = frames.loadFrameData(ctx, frameId)
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)
    val valueDataType: DataType = frame.schema.columns(columnIndex)._2
    val (weightsColumnIndexOption, weightsDataTypeOption) = if (arguments.weightsColumn.isEmpty) {
      (None, None)
    }
    else {
      val weightsColumnIndex = frame.schema.columnIndex(arguments.weightsColumn.get)
      (Some(weightsColumnIndex), Some(frame.schema.columns(weightsColumnIndex)._2))
    }
    val modeCountOption = arguments.maxModesReturned

    ColumnStatistics.columnMode(columnIndex,
      valueDataType,
      weightsColumnIndexOption,
      weightsDataTypeOption,
      modeCountOption,
      rdd)
  }
}
