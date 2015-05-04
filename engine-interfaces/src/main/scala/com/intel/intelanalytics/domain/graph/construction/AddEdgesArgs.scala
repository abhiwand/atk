//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.domain.graph.construction

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.schema.GraphSchema

/**
 * Arguments for adding Edges to a Edge Frame
 *
 * @param edgeFrame the frame being operated on
 * @param sourceFrame source for the data
 * @param columnNameForSourceVertexId column name for the user defined "id" that uniquely identifies each vertex
 * @param columnNameForDestVertexId column name for the user defined "id" that uniquely identifies each vertex
 * @param columnNames column names to be used as properties for each vertex,
 *                    None means use all columns,
 *                    empty list means use none.
 * @param createMissingVertices true to create extra vertices if needed
 */
case class AddEdgesArgs(edgeFrame: FrameReference,
                        sourceFrame: FrameReference,
                        columnNameForSourceVertexId: String,
                        columnNameForDestVertexId: String,
                        columnNames: Option[Seq[String]] = None,
                        createMissingVertices: Option[Boolean] = Some(false)) {
  require(edgeFrame != null, "edge frame is required")
  require(sourceFrame != null, "source frame is required")
  require(columnNameForSourceVertexId != null, "column name for source vertex id is required to create edges")
  require(columnNameForDestVertexId != null, "column name for destination vertex id is required to create edges")
  allColumnNames.foreach(name => require(!GraphSchema.isEdgeSystemColumn(name), s"$name can't be used as an input column name, it is reserved for system use"))

  /**
   * All of the column names (idColumn plus the rest)
   */
  def allColumnNames: List[String] = {
    List(columnNameForSourceVertexId, columnNameForDestVertexId) ++ columnNames.getOrElse(Nil).toList
  }

  /**
   * true to create extra vertices if needed (converts None to false)
   */
  def isCreateMissingVertices: Boolean = createMissingVertices.getOrElse(false)

}
