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

package com.intel.intelanalytics.engine.spark.frame.plugins.sortedk

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin

// Implicits needed for JSON conversion
object SortedKJsonFormat {
  implicit val sortedKFormat = jsonFormat4(SortedKArgs)
}

import SortedKJsonFormat._

/**
 * Plugin that returns the top-K rows in a frame ordered by column(s).
 *
 * This plugin is more efficient than sorting the entire frame when
 * K is much smaller than the total number of rows in a frame.
 *
 * The results of this plugin are stored in a new frame.
 */
class SortedKPlugin extends SparkCommandPlugin[SortedKArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/sorted_k"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: SortedKArgs)(implicit invocation: Invocation) = 2

  /**
   * Plugin that returns the top-K rows in a frame ordered by column(s).
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return New frame with top-K sorted rows.
   */
  override def execute(arguments: SortedKArgs)(implicit invocation: Invocation): FrameEntity = {
    // load frame
    val frame: SparkFrameData = resolve(arguments.frame)
    val frameRdd = frame.data

    // return new frame with top-k sorted records
    val sortedKFrame = SortedKFunctions.takeOrdered(
      frameRdd,
      arguments.k,
      arguments.columnNamesAndAscending,
      arguments.reduceTreeDepth
    )

    // save the new frame
    val frames = engine.frames
    frames.tryNewFrame(CreateEntityArgs(description = Some("created by sorted_k command"))) {
      newFrame => frames.saveFrameData(newFrame.toReference, sortedKFrame)
    }
  }
}
