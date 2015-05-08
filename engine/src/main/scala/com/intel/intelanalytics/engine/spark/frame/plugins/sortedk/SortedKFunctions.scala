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

package com.intel.intelanalytics.engine.spark.frame.plugins.sortedk

import org.apache.spark.frame.FrameRdd
import org.apache.spark.frame.ordering.{ FrameOrderingUtils, MultiColumnKeyOrdering }

/**
 * Object with methods for sorting the top-k rows in a frame
 */
object SortedKFunctions extends Serializable {

  /**
   * Return the top-K rows in a frame ordered by column(s).
   *
   * @param frameRdd Frame to sort
   * @param k Number of sorted records to return
   * @param columnNamesAndAscending Column names to sort by, true for ascending, false for descending
   * @param reduceTreeDepth Depth of reduce tree (governs number of rounds of reduce tasks)
   * @return New frame with top-K rows
   */
  def takeOrdered(frameRdd: FrameRdd,
                  k: Int,
                  columnNamesAndAscending: List[(String, Boolean)],
                  reduceTreeDepth: Option[Int] = None): FrameRdd = {
    require(k > 0, "k should be greater than zero") //TODO: Should we add an upper bound for K
    require(columnNamesAndAscending != null && columnNamesAndAscending.length > 0, "one or more columnNames is required")

    val columnNames = columnNamesAndAscending.map(_._1)
    val ascendingPerColumn = columnNamesAndAscending.map(_._2)

    val pairRdd = frameRdd.mapRows(row => (row.values(columnNames), row.data))
    implicit val keyOrdering = new MultiColumnKeyOrdering(ascendingPerColumn)

    val topRows = if (reduceTreeDepth.isDefined) {
      FrameOrderingUtils.takeOrderedTree(pairRdd, k, reduceTreeDepth.get)
    }
    else {
      FrameOrderingUtils.takeOrderedTree(pairRdd, k)
    }

    // ascending is always true here because we control in the ordering
    val topRowsRdd = frameRdd.sparkContext.parallelize(topRows).map { case (key, row) => row }
    new FrameRdd(frameRdd.frameSchema, topRowsRdd)
  }
}
