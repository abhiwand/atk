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

package com.intel.graphbuilder.schema

import org.apache.commons.lang3.StringUtils

/**
 * A property definition is either of type Edge or Vertex
 */
object PropertyType extends Enumeration {
  val Vertex, Edge = Value
}

/**
 * Schema definition for a Property
 *
 * @param propertyType this property is either for a Vertex or an Edge
 * @param name property name
 * @param dataType data type
 * @param unique True if this property is unique
 * @param indexed True if this property should be indexed
 */
case class PropertyDef(propertyType: PropertyType.Value, name: String, dataType: Class[_], unique: Boolean, indexed: Boolean) {

  if (StringUtils.isEmpty(name)) {
    throw new IllegalArgumentException("property name can't be empty")
  }

  // TODO: in the future, possibly add support for indexName

}