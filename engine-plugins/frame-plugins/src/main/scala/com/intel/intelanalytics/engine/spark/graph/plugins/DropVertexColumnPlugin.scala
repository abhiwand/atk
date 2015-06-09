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

package com.intel.intelanalytics.engine.spark.graph.plugins

import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.domain.schema.{ GraphSchema, VertexSchema }
import com.intel.intelanalytics.engine.spark.frame.plugins.DropColumnsPlugin
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
import com.intel.intelanalytics.domain.frame.{ FrameReference, FrameEntity, DropColumnsArgs }
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.ExecutionContext

/**
 * Drop columns from vertex frame.
 */
@PluginDoc(oneLine = "Drop columns from vertex frame.",
  extended = "",
  returns = "")
class DropVertexColumnPlugin extends DropColumnsPlugin {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:vertex/drop_columns"

  val systemFields = Set(GraphSchema.vidProperty, GraphSchema.labelProperty)

  override def execute(arguments: DropColumnsArgs)(implicit invocation: Invocation): FrameEntity = {
    val frames = engine.frames

    // validate arguments
    val frame = frames.expectFrame(arguments.frame)
    // validation only
    frame.schema.dropColumns(arguments.columns)
    DropVertexColumnPlugin.rejectInvalidColumns(arguments.columns, systemFields)

    // let the frame plugin do the actual work
    super.execute(arguments)
  }

}

object DropVertexColumnPlugin {

  def rejectInvalidColumns(columns: List[String], invalidColumns: Set[String]) {
    val invalid = columns.filter(s => invalidColumns.contains(s))

    if (invalid.nonEmpty) {
      val canNotDrop = invalid.mkString(",")
      throw new IllegalArgumentException(s"The following columns are not allowed to be dropped: $canNotDrop")
    }
  }

}
