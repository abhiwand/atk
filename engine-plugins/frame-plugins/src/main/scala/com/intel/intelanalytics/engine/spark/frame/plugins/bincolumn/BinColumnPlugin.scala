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

package com.intel.intelanalytics.engine.spark.frame.plugins.bincolumn

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData, LegacyFrameRdd }
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
import org.apache.spark.frame.FrameRdd

import scala.concurrent.{ Await, ExecutionContext }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Column values into bins.
 *
 * Two types of binning are provided: equalwidth and equaldepth.
 *
 * Equal width binning places column values into bins such that the values in each bin fall within the same
 * interval and the interval width for each bin is equal.
 *
 * Equal depth binning attempts to place column values into bins such that each bin contains the same number
 * of elements
 *
 * Parameters
 * ----------
 * column_name : str
 *   The column whose values are to be binned.
 * cutoffs : array of values
 *   Array of values containing bin cutoff points.
 *   Array can be list or tuple.
 *   Array values must be progressively increasing.
 *   All bin boundaries must be included, so, with N bins, you need N+1 values.
 * include_lowest : bool (optional)
 *   Specify how the boundary conditions are handled.
 *   True indicates that the lower bound of the bin is inclusive.
 *   False indicates that the upper bound is inclusive.
 *   Default is True.
 * strict_binning : bool (optional)
 *   Specify how values outside of the cutoffs array should be binned.
 *   If set to True, each value less than cutoffs[0] or greater than
 *   cutoffs[-1] will be assigned a bin value of -1.
 *   If set to False, values less than cutoffs[0] will be included in the first
 *   bin while values greater than cutoffs[-1] will be included in the final
 *   bin.
 *   Default is False.
 * bin_column_name : str (optional)
 *   The name for the new binned column.
 *   Default is ``<column_name>_binned``.
 */
@PluginDoc(oneLine = "Classify data into user-defined groups.",
  extended = """Summarize rows of data based on the value in a single column by sorting them
into bins, or groups, based on a list of bin cutoff points.

Notes
-----
1)  Unicode in column names is not supported and will likely cause the
    drop_frames() method (and others) to fail!
2)  Bins IDs are 0-index: the lowest bin number is 0.
3)  The first and last cutoffs are always included in the bins.
    When include_lowest is ``True``, the last bin includes both cutoffs.
    When include_lowest is ``False``, the first bin (bin 0) includes both
    cutoffs.""")
class BinColumnPlugin extends SparkCommandPlugin[BinColumnArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/bin_column"

  /**
   * Column values into bins.
   *
   * Two types of binning are provided: equalwidth and equaldepth.
   *
   * Equal width binning places column values into bins such that the values in each bin fall within the same
   * interval and the interval width for each bin is equal.
   *
   * Equal depth binning attempts to place column values into bins such that each bin contains the same number
   * of elements
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: BinColumnArgs)(implicit invocation: Invocation): FrameEntity = {
    val frame: SparkFrameData = resolve(arguments.frame)
    val columnIndex = frame.meta.schema.columnIndex(arguments.columnName)
    val columnType = frame.meta.schema.columnDataType(arguments.columnName)
    require(columnType.isNumerical, s"Invalid column ${arguments.columnName} for bin column.  Expected a numerical data type, but got $columnType.")
    val binColumnName = arguments.binColumnName.getOrElse(frame.meta.schema.getNewColumnName(arguments.columnName + "_binned"))
    if (frame.meta.schema.hasColumn(binColumnName))
      throw new IllegalArgumentException(s"Duplicate column name: ${arguments.binColumnName}")

    // run the operation and save results
    val updatedSchema = frame.meta.schema.addColumn(binColumnName, DataTypes.int32)
    val rdd = frame.data
    val binnedRdd = DiscretizationFunctions.binColumns(columnIndex, arguments.cutoffs,
      arguments.includeLowest.getOrElse(true), arguments.strictBinning.getOrElse(false), rdd)

    save(new SparkFrameData(frame.meta.withSchema(updatedSchema), new FrameRdd(updatedSchema, binnedRdd))).meta
  }
}
