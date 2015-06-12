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

import com.intel.intelanalytics.domain.schema.GraphSchema
import com.intel.intelanalytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.intelanalytics.engine.spark.frame.plugins.RenameColumnsPlugin
import com.intel.intelanalytics.domain.frame.{ FrameEntity, RenameColumnsArgs }

/**
 * Rename columns for vertex frame.
 */
@PluginDoc(oneLine = "Rename columns for vertex frame.",
  extended = "",
  returns = "")
class RenameVertexColumnsPlugin extends RenameColumnsPlugin {
  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:vertex/rename_columns"

  val systemFields = Set(GraphSchema.vidProperty, GraphSchema.labelProperty)

  override def execute(arguments: RenameColumnsArgs)(implicit invocation: Invocation): FrameEntity = {
    rejectInvalidColumns(arguments.names.keys, systemFields)
    super.execute(arguments)
  }

  def rejectInvalidColumns(columns: Iterable[String], invalidColumns: Set[String]) {
    val invalid = columns.filter(s => invalidColumns.contains(s))

    if (invalid.nonEmpty) {
      val cannotRename = invalid.mkString(",")
      throw new IllegalArgumentException(s"The following columns are not allowed to be renamed: $cannotRename")
    }
  }
}
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
