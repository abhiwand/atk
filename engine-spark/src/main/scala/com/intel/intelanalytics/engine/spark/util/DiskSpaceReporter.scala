package com.intel.intelanalytics.engine.spark.util

import org.apache.commons.lang3.StringUtils
import sys.process.Process
import com.intel.event.EventLogging

object DiskSpaceReporter extends EventLogging {

  private val diskSpaceErrorThreshold = 99
  private val diskSpaceWarningThreshold = 90
  private val diskSpaceInfoThreshold = 50

  /**
   * Log disk space available
   */
  def checkDiskSpace(): Unit = withContext("DiskSpaceReporter") {
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
