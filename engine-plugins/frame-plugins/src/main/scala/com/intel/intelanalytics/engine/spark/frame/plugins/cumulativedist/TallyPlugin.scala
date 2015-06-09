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

package com.intel.intelanalytics.engine.spark.frame.plugins.cumulativedist

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ TallyArgs, FrameEntity }
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
import com.intel.intelanalytics.engine.spark.frame.LegacyFrameRdd
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Computes a cumulative count
 *
 * Parameters
 * ----------
 * sample_col : str
 * The name of the column from which to compute the cumulative count.
 * count_value : str
 * The column value to be used for the counts.
 */
@PluginDoc(oneLine = "Count number of times a value is seen.",
  extended = """A cumulative count is computed by sequentially stepping through the column
values and keeping track of the the number of times the specified
*count_value* has been seen up to the current value.""")
class TallyPlugin extends SparkCommandPlugin[TallyArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/tally"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * Computes a cumulative count
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: TallyArgs)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    // validate arguments
    val frameRef = arguments.frame
    val frameEntity = frames.expectFrame(frameRef)
    val sampleIndex = frameEntity.schema.columnIndex(arguments.sampleCol)

    // run the operation
    val frameRdd = frames.loadLegacyFrameRdd(sc, frameEntity)
    val (cumulativeDistRdd, columnName) = (CumulativeDistFunctions.cumulativeCount(frameRdd, sampleIndex, arguments.countVal), "_tally")
    val frameSchema = frameEntity.schema
    val updatedSchema = frameSchema.addColumn(arguments.sampleCol + columnName, DataTypes.float64)

    // save results
    frames.saveLegacyFrame(frameEntity.toReference, new LegacyFrameRdd(updatedSchema, cumulativeDistRdd))
  }
}
