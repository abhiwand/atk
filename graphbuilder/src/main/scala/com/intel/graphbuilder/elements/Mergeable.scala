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
 * Mergeable items can be combined into a new single item.
 * <p>
 * In the case of Graph Elements, merging means creating a new Graph Element that has a combined set of properties.
 * </p>
 */
trait Mergeable[T] {

  /**
   * Merge-ables with the same id can be merged together.
   *
   * (In Spark, you would use this as the unique id in the groupBy before merging duplicates)
   */
  def id: Any

  /**
   * Merge two items into one.
   *
   * @param other item to merge
   * @return the new merged item
   */
  def merge(other: T): T

}
