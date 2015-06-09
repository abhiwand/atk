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
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

/**
 * Drop columns from edge frame.
 */
@PluginDoc(oneLine = "Drop columns from edge frame.",
  extended = "",
  returns = "")
class DropEdgeColumnPlugin extends DropVertexColumnPlugin {
  override def name: String = "frame:edge/drop_columns"
  override val systemFields = Set(GraphSchema.edgeProperty, GraphSchema.srcVidProperty, GraphSchema.destVidProperty, GraphSchema.labelProperty)
}
