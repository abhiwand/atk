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
import com.intel.intelanalytics.domain.schema.DataTypes

import scala.util.Try

/**
 * A property on a Vertex or Edge.
 *
 * @param key the name of the property
 * @param value the value of the property
 */
case class Property(key: String, value: Any) {

  /**
   * Convenience constructor
   */
  def this(key: Any, value: Any) = {
    this(StringUtils.nullSafeToString(key), value)
  }
}

object Property {

  /**
   * Merge two set of properties so that keys appear once.
   *
   * Conflicts are handled arbitrarily.
   */
  def merge(setA: Set[Property], setB: Set[Property]): Set[Property] = {
    val unionPotentialKeyConflicts = setA ++ setB
    val mapWithoutDuplicates = unionPotentialKeyConflicts.map(p => (p.key, p)).toMap
    mapWithoutDuplicates.valuesIterator.toSet
  }

}

/**
 * An ordering of properties by key and value
 *
 * Can be used to enable Spark's sort-based shuffle which is more memory-efficient.
 */
object PropertyOrdering extends Ordering[Property] {
  def compare(a: Property, b: Property) = {
    val keyComparison = a.key compare b.key
    if (keyComparison == 0) {
      Try(DataTypes.compare(a.value, b.value))
        .getOrElse(a.value.toString() compare b.value.toString)
    }
    else keyComparison
  }
}
