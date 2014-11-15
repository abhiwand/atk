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

package com.intel.intelanalytics.domain.schema

/**
 * Utility methods and constants used in Schemas for Seamless Graphs (not Titan)
 *
 * "Seamless Graph" is a graph that provides a "seamless user experience" between graphs and frames.
 * The same data can be treated as frames one moment and as a graph the next without any import/export.
 */
object GraphSchema {

  val vertexSystemColumns = Column("_vid", DataTypes.int64) :: Column("_label", DataTypes.string) :: Nil
  /** ordered list */
  val vertexSystemColumnNames = vertexSystemColumns.map(column => column.name)
  val vertexSystemColumnNamesSet = vertexSystemColumnNames.toSet
  val edgeSystemColumns = Column("_eid", DataTypes.int64) ::
    Column("_src_vid", DataTypes.int64) ::
    Column("_dest_vid", DataTypes.int64) ::
    Column("_label", DataTypes.string) ::
    Nil

  /** ordered list */
  val edgeSystemColumnNames = edgeSystemColumns.map(column => column.name)
  val edgeSystemColumnNamesSet = edgeSystemColumnNames.toSet

  def isVertexSystemColumn(name: String): Boolean = {
    vertexSystemColumnNames.contains(name)
  }

  def isEdgeSystemColumn(name: String): Boolean = {
    edgeSystemColumnNames.contains(name)
  }
}
