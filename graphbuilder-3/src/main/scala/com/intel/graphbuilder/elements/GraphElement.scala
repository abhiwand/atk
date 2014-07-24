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
   * Get a property value if this key exists and the value is String type
   * @param key Property key
   * @return Matching property value, or empty string is no such property
   */
  def getStringPropertyValue(key: String): String

}
