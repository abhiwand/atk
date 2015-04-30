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

/**
 * A Vertex.
 * <p>
 * GB Id's are special properties that uniquely identify Vertices.  They are special, in that they must uniquely
 * identify vertices, but otherwise they are completely normal properties.
 * </p>
 * @constructor create a new Vertex
 * @param physicalId the unique Physical ID for the Vertex from the underlying Graph storage layer (optional)
 * @param gbId the unique id that will be used by graph builder
 * @param properties the other properties that exist on this vertex
 */
case class GBVertex(physicalId: Any, gbId: Property, properties: Set[Property]) extends GraphElement with Mergeable[GBVertex] {

  def this(gbId: Property, properties: Set[Property]) {
    this(null, gbId, properties)
  }

  if (gbId == null) {
    throw new IllegalArgumentException("gbId can't be null")
  }

  /**
   * Merge-ables with the same id can be merged together.
   *
   * (In Spark, you would use this as the unique id in the groupBy before merging duplicates)
   */
  override def id: Any = gbId

  /**
   * Merge two Vertices with the same id into a single Vertex with a combined list of properties.
   *
   * Conflicts are handled arbitrarily.
   *
   * @param other item to merge
   * @return the new merged item
   */
  override def merge(other: GBVertex): GBVertex = {
    if (id != other.id) {
      throw new IllegalArgumentException("You can't merge vertices with different ids")
    }

    if (physicalId != null && other.physicalId == null) {
      new GBVertex(physicalId, gbId, Property.merge(this.properties, other.properties))
    }
    else if (physicalId == null && other.physicalId != null) {
      new GBVertex(other.physicalId, gbId, Property.merge(this.properties, other.properties))
    }
    else if (physicalId != null && physicalId == other.physicalId) {
      new GBVertex(physicalId, gbId, Property.merge(this.properties, other.properties))
    }
    else {
      new GBVertex(gbId, Property.merge(this.properties, other.properties))
    }
  }

  /**
   * Full set of properties including the gbId
   */
  def fullProperties: Set[Property] = {
    properties + gbId
  }

  /**
   * Find a property in the property list by key
   *
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
