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
package org.apache.spark.frame.ordering

import org.apache.spark.sql

/**
 * Ordering for sorting key-value RDDs by key
 *
 * The key is a list of column values. Each column is ordered in ascending or descending order.
 *
 * @param ascendingPerColumn Indicates whether to sort each column in list in ascending (true)
 *                           or descending (false) order
 */
class MultiColumnKeyOrdering(ascendingPerColumn: List[Boolean]) extends Ordering[(List[Any], sql.Row)] {
  val multiColumnOrdering = new MultiColumnOrdering(ascendingPerColumn)

  override def compare(a: (List[Any], sql.Row), b: (List[Any], sql.Row)): Int = {
    multiColumnOrdering.compare(a._1, b._1)
  }
}
