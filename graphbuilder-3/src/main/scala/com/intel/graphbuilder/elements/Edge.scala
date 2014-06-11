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

package com.intel.graphbuilder.elements

import com.intel.graphbuilder.util.StringUtils

/**
 * An Edge links two Vertices.
 * <p>
 * GB Id's are special properties that uniquely identify Vertices.  They are special, in that they must uniquely
 * identify vertices, but otherwise they are completely normal properties.
 * </p>
 * @param tailPhysicalId the unique Physical ID for the source Vertex from the underlying Graph storage layer (optional)
 * @param headPhysicalId the unique Physical ID for the destination Vertex from the underlying Graph storage layer (optional)
 * @param tailVertexGbId the unique ID for the source Vertex
 * @param headVertexGbId the unique ID for the destination Vertex
 * @param label the Edge label
 * @param properties the list of properties associated with this edge
 */
case class Edge(var tailPhysicalId: Any, var headPhysicalId: Any, tailVertexGbId: Property, headVertexGbId: Property, label: String, properties: Seq[Property]) extends GraphElement with Mergeable[Edge] {

  def this(tailVertexGbId: Property, headVertexGbId: Property, label: String, properties: Seq[Property]) {
    this(null, null, tailVertexGbId, headVertexGbId, label, properties)
  }

  def this(tailVertexGbId: Property, headVertexGbId: Property, label: Any, properties: Seq[Property]) {
    this(tailVertexGbId, headVertexGbId, StringUtils.nullSafeToString(label), properties)
  }

  /**
   * Merge-ables with the same id can be merged together.
   *
   * (In Spark, you would use this as the unique id in the groupBy before merging duplicates)
   */
  override def id: Any = {
    (tailVertexGbId, headVertexGbId, label)
  }

  /**
   * Merge properties for two edges.
   *
   * Conflicts are handled arbitrarily.
   *
   * @param other item to merge
   * @return the new merged item
   */
  override def merge(other: Edge): Edge = {
    if (id != other.id) {
      throw new IllegalArgumentException("You can't merge edges with different ids or labels")
    }
    new Edge(tailVertexGbId, headVertexGbId, label, Property.merge(this.properties, other.properties))
  }

  /**
   * Create edge with head and tail in reverse order
   */
  def reverse(): Edge = {
    new Edge(headVertexGbId, tailVertexGbId, label, properties)
  }

}
