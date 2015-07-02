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

package com.intel.taproot.analytics.engine.spark.graph.plugins

import com.intel.taproot.analytics.domain.schema.GraphSchema
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, PluginDoc }

/**
 * Rename columns for edge frame.
 */
@PluginDoc(oneLine = "Rename columns for edge frame.",
  extended = "",
  returns = "")
class RenameEdgeColumnsPlugin extends RenameVertexColumnsPlugin {
  override def name: String = "frame:edge/rename_columns"
  override val systemFields = Set(GraphSchema.edgeProperty, GraphSchema.srcVidProperty, GraphSchema.destVidProperty, GraphSchema.labelProperty)
}
