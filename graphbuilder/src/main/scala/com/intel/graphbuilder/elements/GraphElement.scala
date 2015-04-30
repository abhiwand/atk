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
 * Marker interface for GraphElements (Vertices and Edges).
 * <p>
 * This can be used if you want to parse both Edges and Vertices in one pass over the input into a single stream.
 * </p>
 */
trait GraphElement {

  /**
   * Find a property in the property list by key
   * @param key Property key
   * @return Matching property
   */
  def getProperty(key: String): Option[Property]

  /**
   * Get a property value as String if this key exists
   * @param key Property key
   * @return Matching property value, or empty string if no such property
   */
  def getPropertyValueAsString(key: String): String

  /**
   * Get property values to a array in the order specified.
   * @param columns specifed columns to retrieve property values
   * @param properties properties
   * @return array of column values
   */
  def getPropertiesValueByColumns(columns: List[String], properties: Set[Property]): Array[Any] = {
    val mapping = properties.map(p => p.key -> p.value).toMap
    columns.map(c => mapping(c)).toArray
  }
}
