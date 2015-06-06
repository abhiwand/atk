/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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
