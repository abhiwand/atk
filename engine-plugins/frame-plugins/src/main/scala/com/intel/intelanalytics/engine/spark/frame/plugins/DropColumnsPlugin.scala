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

import com.intel.intelanalytics.domain.frame.{ DropColumnsArgs, FrameEntity }
import com.intel.intelanalytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

@PluginDoc(oneLine = "Remove columns from the frame.",
  extended = """The data from the columns is lost.

Notes
-----
It is not possible to delete all columns from a frame.
At least one column needs to remain.
If it is necessary to delete all columns, then delete the frame.""")
class DropColumnsPlugin extends SparkCommandPlugin[DropColumnsArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/drop_columns"

  /**
   * Remove columns from a frame.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DropColumnsArgs)(implicit invocation: Invocation): FrameEntity = {
    val frame: SparkFrameData = resolve(arguments.frame)
    val schema = frame.meta.schema
    schema.validateColumnsExist(arguments.columns)

    require(schema.columnNamesExcept(arguments.columns).length > 0,
      "Cannot delete all columns, please leave at least one column remaining")

    // run the operation
    val result = frame.data.selectColumns(schema.columnNamesExcept(arguments.columns))
    assert(result.frameSchema.columnNames.intersect(arguments.columns).isEmpty, "Column was not removed from schema!")

    // save results
    save(new SparkFrameData(frame.meta, result)).meta
  }
}
