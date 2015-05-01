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
 * @param properties the set of properties associated with this edge
 */
case class GBEdge(eid: Option[Long], var tailPhysicalId: Any, var headPhysicalId: Any, tailVertexGbId: Property, headVertexGbId: Property, label: String, properties: Set[Property]) extends GraphElement with Mergeable[GBEdge] {

  def this(eid: Option[Long], tailVertexGbId: Property, headVertexGbId: Property, label: String, properties: Set[Property]) {
    this(eid, null, null, tailVertexGbId, headVertexGbId, label, properties)
  }

  def this(eid: Option[Long], tailVertexGbId: Property, headVertexGbId: Property, label: Any, properties: Set[Property]) {
    this(eid, tailVertexGbId, headVertexGbId, StringUtils.nullSafeToString(label), properties)
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
  override def merge(other: GBEdge): GBEdge = {
    if (id != other.id) {
      throw new IllegalArgumentException("You can't merge edges with different ids or labels")
    }
    new GBEdge(eid, tailVertexGbId, headVertexGbId, label, Property.merge(this.properties, other.properties))
  }

  /**
   * Create edge with head and tail in reverse order
   */
  def reverse(): GBEdge = {
    new GBEdge(eid, headVertexGbId, tailVertexGbId, label, properties)
  }

  /**
   * Find a property in the property list by key
   * @param key Property key
   * @return Matching property
   */
  override def getProperty(key: String): Option[Property] = {
    properties.find(p => p.key == key)
  }

  /**
   * Get a property value as String if this key exists
   * @param key Property key
   * @return Matching property value, or empty string if no such property
   */
  override def getPropertyValueAsString(key: String): String = {
    val result = for {
      property <- this.getProperty(key)
    } yield property.value.toString
    result.getOrElse("")
  }
}
