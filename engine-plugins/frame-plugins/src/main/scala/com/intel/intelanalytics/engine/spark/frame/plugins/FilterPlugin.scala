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

import com.intel.intelanalytics.domain.FilterArgs
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData, PythonRddStorage }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Select all rows which satisfy a predicate
 */
@PluginDoc(oneLine = "",
  extended = "",
  returns = "")
class FilterPlugin extends SparkCommandPlugin[FilterArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/filter"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: FilterArgs)(implicit invocation: Invocation) = 2

  /**
   * Select all rows which satisfy a predicate
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: FilterArgs)(implicit invocation: Invocation): FrameEntity = {
    val frame: SparkFrameData = resolve(arguments.frame)

    val updatedRdd = PythonRddStorage.mapWith(frame.data, arguments.udf, sc = sc)
    engine.frames.saveFrameData(frame.meta.toReference, updatedRdd)
  }
}
