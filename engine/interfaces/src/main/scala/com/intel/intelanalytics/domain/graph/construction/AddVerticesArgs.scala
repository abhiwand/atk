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

package com.intel.intelanalytics.domain.graph.construction

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.schema.GraphSchema

/**
 * Arguments for adding Vertices to a Vertex Frame
 *
 * @param vertexFrame the frame being operated on
 * @param sourceFrame source for the data
 * @param idColumnName column name for the user defined "id" that uniquely identifies each vertex
 * @param columnNames column names to be used as properties for each vertex,
 *                    None means use all columns,
 *                    empty list means use none.
 */
case class AddVerticesArgs(vertexFrame: FrameReference, sourceFrame: FrameReference, idColumnName: String, columnNames: Option[Seq[String]] = None) {
  allColumnNames.foreach(name => require(!GraphSchema.isVertexSystemColumn(name), s"$name can't be used as an input column name, it is reserved for system use"))

  /**
   * All of the column names (idColumn plus the rest)
   */
  def allColumnNames: List[String] = {
    if (columnNames.isDefined) {
      List(idColumnName) ++ columnNames.get.toList
    }
    else {
      List(idColumnName)
    }
  }

}
