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

import com.intel.intelanalytics.domain.schema.DataTypes

/**
 * Ordering for sorting frame RDDs by multiple columns
 *
 * Each column is ordered in ascending or descending order.
 *
 * @param ascendingPerColumn Indicates whether to sort each column in list in ascending (true)
 *                           or descending (false) order
 */
class MultiColumnOrdering(ascendingPerColumn: List[Boolean]) extends Ordering[List[Any]] {
  override def compare(a: List[Any], b: List[Any]): Int = {
    for (i <- 0 to a.length - 1) {
      val columnA = a(i)
      val columnB = b(i)
      val result = DataTypes.compare(columnA, columnB)
      if (result != 0) {
        if (ascendingPerColumn(i)) {
          // ascending
          return result
        }
        else {
          // descending
          return result * -1
        }
      }
    }
    0
  }
}
