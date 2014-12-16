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

package com.intel.intelanalytics.engine

/**
 * Cycle controller. It is for making sure that an action will only take place after
 * at least the time specified by the cycle since last time it took place.
 */
trait CycleMonitor {
  var lastUpdateTime = System.currentTimeMillis()
  val cycle = 1000 //in milliseconds

  /**
   * Check whether it is eligible to start new cycle
   * @return flag indicating whether it is eligible to move on.
   */
  def isReadyForNextCycle(): Boolean = {
    if (System.currentTimeMillis() - lastUpdateTime > cycle) true else false
  }

  /**
   * Move on to next cycle
   */
  def moveToNextCycle(): Unit = {
    if (!isReadyForNextCycle) {
      val diff = System.currentTimeMillis() - lastUpdateTime
      throw new Exception(s"Not eligible to move on to the next cycle. Eligible in $diff milliseconds.")
    }

    lastUpdateTime = System.currentTimeMillis()
  }
}
