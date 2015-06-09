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
import com.intel.intelanalytics.domain.frame.{ TallyPercentArgs, FrameEntity }
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.spark.frame.LegacyFrameRdd
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Computes a cumulative percent count
 */
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
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    // validate arguments
    val frameRef = arguments.frame
    val frameEntity = frames.expectFrame(frameRef)
    val sampleIndex = frameEntity.schema.columnIndex(arguments.sampleCol)

    // run the operation
    val frameRdd = frames.loadLegacyFrameRdd(sc, frameRef)
    val (cumulativeDistRdd, columnName) = (CumulativeDistFunctions.cumulativePercentCount(frameRdd, sampleIndex, arguments.countVal), "_tally_percent")
    val frameSchema = frameEntity.schema
    val updatedSchema = frameSchema.addColumn(arguments.sampleCol + columnName, DataTypes.float64)

    // save results
    frames.saveLegacyFrame(frameEntity.toReference, new LegacyFrameRdd(updatedSchema, cumulativeDistRdd))
  }
}
