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

package com.intel.intelanalytics.domain.graph

import com.intel.intelanalytics.domain.schema.{ GraphSchema, EdgeSchema }

/**
 * Arguments for defining an edge
 * @param graphRef
 * @param label the label for this edge list
 * @param srcVertexLabel the src "type" of vertices this edge connects
 * @param destVertexLabel the destination "type" of vertices this edge connects
 * @param directed true if edges are directed, false if they are undirected
 */
case class DefineEdgeArgs(graphRef: GraphReference, label: String, srcVertexLabel: String, destVertexLabel: String, directed: Boolean = false) {

  def edgeSchema: EdgeSchema = {
    new EdgeSchema(GraphSchema.edgeSystemColumns, label, srcVertexLabel, destVertexLabel, directed)
  }
}
