//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.graphconstruction.{ EdgeRule, VertexRule, OutputConfiguration }

//TODO: Many of these classes will go away in the future, replaced with something more generic.

//TODO: Add more parameters as appropriate
case class Als[GraphRef](graph: GraphRef, lambda: Double, max_supersteps: Option[Int],
                         converge_threshold: Option[Int], feature_dimension: Option[Int])

case class LoadLines[+Arguments, FrameRef](source: String, destination: FrameRef, skipRows: Option[Int], overwrite: Option[Boolean], lineParser: Partial[Arguments], schema: Schema) {
  require(source != null, "source is required")
  require(destination != null, "destination is required")
  require(skipRows.isEmpty || skipRows.get >= 0, "cannot skip negative number of rows")
  require(lineParser != null, "lineParser is required")
  require(schema != null, "schema is required")
}

case class FilterPredicate[+Arguments, FrameRef](frame: FrameRef, predicate: String) {
  require(frame != null, "frame is required")
  require(predicate != null, "predicate is required")
}

case class FrameRemoveColumn[+Arguments, FrameRef](frame: FrameRef, column: String) {
  require(frame != null, "frame is required")
  require(column != null, "column is required")
}
case class FrameRenameFrame[+Arguments, FrameRef](frame: FrameRef, new_name: String) {
  require(frame != null, "frame is required")
  require(new_name != null && new_name.size > 0, "new_name is required")
}
case class FrameAddColumn[+Arguments, FrameRef](frame: FrameRef, columnname: String, columntype: String, expression: String) {
  require(frame != null, "frame is required")
  require(columnname != null, "column name is required")
  require(columntype != null, "column type is required")
  require(expression != null, "expression is required")
}

/**
 * frame join command
 * @param name name of new dataframe to be created, eg: result
 * @param frames input dataframes for the join operation
 * @param how methods of join. inner, left or right
 */
case class FrameJoin(name: String, frames: List[(Long, String)], how: String) {
  require(frames != null, "frame is required")
  require(frames.length == 2, "Two frames are required for the join operation")
}

case class FrameProject[+Arguments, FrameRef](frame: FrameRef, projected_frame: FrameRef, columns: List[String], new_column_names: List[String]) {
  require(frame != null, "frame is required")
  require(projected_frame != null, "projected frame is required")
  require(columns != null && columns.size > 0, "column is required")
  if (new_column_names != null && new_column_names.size > 0) {
    // TODO - accept a null Json deserialization... for now Python is passing an empty list rather than null
    require(columns.size == new_column_names.size, "number of renamed columns must equal number of columns")
    // TODO - ensure no duplicate names in columns
    // TODO - ensure no duplicate names in renamed_columns
  }
}

case class FrameRenameColumn[+Arguments, FrameRef](frame: FrameRef, originalcolumn: String, renamedcolumn: String) {
  require(frame != null, "frame is required")
  require(originalcolumn != null, "original column is required")
  require(renamedcolumn != null, "renamed column is required")
}

case class SeparatorArgs(separator: Char)

/**
 * Command for loading  graph data into existing graph in the graph database. Source is tabular data from a dataframe
 * and it is converted into graph data using the graphbuilder3 graph construction rules.
 * @param graphRef Handle to the graph to be written to.
 * @param sourceFrameRef Handle to the dataframe to be used as a data source.
 * @param outputConfig The configuration rules specifying how the graph database will be written to.
 * @param vertexRules Specification of how tabular data will be interpreted as vertices.
 * @param edgeRules Specification of how tabular data will be interpreted as edge.
 * @param retainDanglingEdges
 * @param bidirectional Are edges bidirectional or unidirectional? (equivalently, undirected or directed?)
 * @tparam Arguments Type of the command packed provided by the caller.
 * @tparam GraphRef Type of the reference to the graph being written to.
 * @tparam FrameRef Type of the reference to the source frame being read from.
 */
case class GraphLoad[+Arguments, GraphRef, FrameRef](graphRef: GraphRef,
                                                     sourceFrameRef: FrameRef,
                                                     outputConfig: OutputConfiguration,
                                                     vertexRules: List[VertexRule],
                                                     edgeRules: List[EdgeRule],
                                                     retainDanglingEdges: Boolean,
                                                     bidirectional: Boolean) {
  require(graphRef != null)
  require(sourceFrameRef != null)
  require(outputConfig != null)
}

