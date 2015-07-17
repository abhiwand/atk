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

package com.intel.taproot.analytics.engine.frame.plugins.bincolumn

import com.intel.taproot.analytics.domain.frame._
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

// Implicits needed for JSON conversion
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._
import org.apache.spark.SparkContext._

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
 * num_bins : int (optional)
 *   The maximum number of bins.
 *   Default is the Square-root choice
 *   :math:`\lfloor \sqrt{m} \rfloor`, where :math:`m` is the number of rows.
 * bin_column_name : str (optional)
 *   The name for the new column holding the grouping labels.
 *   Default is ``<column_name>_binned``.
 */
@PluginDoc(oneLine = "Classify column into same-width groups.",
  extended = """Group rows of data based on the value in a single column and add a label
to identify grouping.

Equal width binning places column values into groups such that the values
in each group fall within the same interval and the interval width for each
group is equal.

Notes
-----
1)  Unicode in column names is not supported and will likely cause the
    drop_frames() method (and others) to fail!
2)  The num_bins parameter is considered to be the maximum permissible number
    of bins because the data may dictate fewer bins.
    For example, if the column to be binned has 10
    elements with only 2 distinct values and the *num_bins* parameter is
    greater than 2, then the number of actual number of bins will only be 2.
    This is due to a restriction that elements with an identical value must
    belong to the same bin.""",
  returns = "A list of the edges of each bin.")
class BinColumnEqualWidthPlugin extends ComputedBinColumnPlugin {
  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   *
   * The colon ":" is used to to indicate command destination base classes, default classes or classes of a
   * specific storage type:
   *
   * - graph:titan means command is loaded into class TitanGraph
   * - graph: means command is loaded into class Graph, our default type which will be the Parquet-backed graph
   * - graph would mean command is loaded into class BaseGraph, which applies to all graph classes
   * - frame: and means command is loaded in class Frame.  Example: "frame:/assign_sample"
   * - model:logistic_regression  means command is loaded into class LogisticRegressionModel
   */
  override def name: String = "frame/bin_column_equal_width"

  /**
   * @param arguments command arguments: used if a command can produce variable number of jobs
   * @param invocation
   * @return number of jobs in this command
   */
  override def numberOfJobs(arguments: ComputedBinColumnArgs)(implicit invocation: Invocation): Int = 5

  /**
   * Discretize a variable into a finite number of bins
   * @param columnIndex index of column to bin
   * @param numBins number of bins to use
   * @param rdd rdd to bin against
   * @return a result object containing the binned rdd and the list of computed cutoffs
   */
  override def executeBinColumn(columnIndex: Int, numBins: Int, rdd: FrameRdd): RddWithCutoffs = {
    DiscretizationFunctions.binEqualWidth(columnIndex, numBins, rdd)
  }
}
