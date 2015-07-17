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

package com.intel.taproot.analytics.engine.frame.plugins

import com.intel.taproot.analytics.domain.frame.{ RenameFrameArgs, FrameEntity }
import com.intel.taproot.analytics.engine.frame.Frame
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, CommandPlugin, Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.spark.frame.SparkFrame

// Implicits needed for JSON conversion
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

/**
 * Parameters
 * ----------
 * new_name : str
 *   the new name of the frame.
 */

/**
 * Rename a frame
 */
@PluginDoc(oneLine = "Change the name of the current frame.",
  extended = """Set the name of this frame.""")
class RenameFramePlugin extends CommandPlugin[RenameFrameArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/rename"

  /**
   * Rename a frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: RenameFrameArgs)(implicit invocation: Invocation): FrameEntity = {
    val frame: Frame = arguments.frame
    frame.name = arguments.newName
    frame
  }
}
