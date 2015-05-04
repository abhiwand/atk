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

package com.intel.intelanalytics.engine.spark.util

import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.engine.plugin.Invocation
import org.apache.commons.lang3.StringUtils
import sys.process.Process
import com.intel.event.EventLogging

object DiskSpaceReporter extends EventLogging with EventLoggingImplicits {

  private val diskSpaceErrorThreshold = 99
  private val diskSpaceWarningThreshold = 90
  private val diskSpaceInfoThreshold = 50

  /**
   * Log disk space available
   */
  def checkDiskSpace()(implicit invocation: Invocation): Unit = withContext("DiskSpaceReporter") {
    try {
      Process("df -h").lines.drop(1).foreach(line => {
        val parts = line.split("[ ]+")
        val mountPoint = parts(5)
        val percentUsed = StringUtils.stripEnd(parts(4), "%").toInt
        if (percentUsed >= diskSpaceErrorThreshold) {
          error(mountPoint + " is " + percentUsed + "% full - this is unusually high and may indicate a problem!!!")
        }
        else if (percentUsed >= diskSpaceWarningThreshold) {
          warn(mountPoint + " is " + percentUsed + "% full - this is unusually high and may indicate a problem!!!")
        }
        else if (percentUsed >= diskSpaceInfoThreshold) {
          info(mountPoint + " is " + percentUsed + "% full")
        }
      })
    }
    catch {
      case e: Exception => warn("could not calculate free disk space")
    }
  }
}
