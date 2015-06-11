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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.domain.LongValue
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.FrameNoArgs
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation, PluginDoc, ArgDoc }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Get the RDD partition count after loading a frame (useful for debugging purposes)
 */
@PluginDoc(oneLine = "Get the RDD partition count after loading a frame.",
  extended = "",
  returns = "")
class SizeOnDiskPlugin extends CommandPlugin[FrameNoArgs, LongValue] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/_size_on_disk"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc("Calls underlying Spark method.", None))

  /**
   * Get the RDD size on disk after loading a frame (useful for debugging/benchmarking purposes)
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: FrameNoArgs)(implicit invocation: Invocation): LongValue = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    // validate arguments
    val frame = frames.expectFrame(arguments.frame)

    frames.getSizeInBytes(frame) match {
      case Some(size) => LongValue(size)
      case _ => throw new RuntimeException(s"Unable to calculate size of frame! Frame is empty or has not been materialized.")
    }
  }
}
