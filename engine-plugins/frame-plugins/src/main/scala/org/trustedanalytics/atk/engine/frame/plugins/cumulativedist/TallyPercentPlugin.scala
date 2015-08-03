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

package org.trustedanalytics.atk.engine.frame.plugins.cumulativedist

import org.trustedanalytics.atk.domain.frame.{ TallyPercentArgs, FrameEntity }
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Computes a cumulative percent count
 *
 * Parameters
 * ----------
 * sample_col : str
 * The name of the column from which to compute the cumulative sum.
 * count_value : str
 * The column value to be used for the counts.
 */
@PluginDoc(oneLine = "Compute a cumulative percent count.",
  extended = """A cumulative percent count is computed by sequentially stepping through
the column values and keeping track of the current percentage of the
total number of times the specified *count_value* has been seen up to
the current value.""")
class TallyPercentPlugin extends SparkCommandPlugin[TallyPercentArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/tally_percent"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * Computes a cumulative percent count
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: TallyPercentArgs)(implicit invocation: Invocation): FrameEntity = {
    val frame: SparkFrame = arguments.frame
    val sampleIndex = frame.schema.columnIndex(arguments.sampleCol)

    // run the operation
    val (cumulativeDistRdd, columnName) = (CumulativeDistFunctions.cumulativePercentCount(frame.rdd, sampleIndex, arguments.countVal), "_tally_percent")
    val updatedSchema = frame.schema.addColumn(arguments.sampleCol + columnName, DataTypes.float64)
    frame.save(new FrameRdd(updatedSchema, cumulativeDistRdd))
  }
}
