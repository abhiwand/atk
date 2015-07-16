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

package com.intel.taproot.analytics.engine.spark.frame.plugins.bincolumn

import com.intel.taproot.analytics.domain.frame._
import com.intel.taproot.analytics.domain.schema.{ Schema, DataTypes }
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.spark.frame.{ SparkFrame, SparkFrameData }
import com.intel.taproot.analytics.engine.spark.plugin.{ SparkCommandPlugin }
import org.apache.spark.frame.FrameRdd

// Implicits needed for JSON conversion
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

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
    val frame: SparkFrame = arguments.frame
    val columnIndex = frame.schema.columnIndex(arguments.columnName)
    frame.schema.requireColumnIsNumerical(arguments.columnName)
    val binColumnName = arguments.binColumnName.getOrElse(frame.schema.getNewColumnName(arguments.columnName + "_binned"))

    // run the operation and save results
    val updatedSchema = frame.schema.addColumn(binColumnName, DataTypes.int32)
    val binnedRdd = DiscretizationFunctions.binColumns(columnIndex, arguments.cutoffs,
      arguments.includeLowest.getOrElse(true), arguments.strictBinning.getOrElse(false), frame.rdd)

    frame.save(new FrameRdd(updatedSchema, binnedRdd))
  }
}
