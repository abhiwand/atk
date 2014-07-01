package com.intel.intelanalytics.engine

/**
 * represent extra progress information
 * @param successTasks current number of succeeded tasks
 * @param failedTasks current number of failed tasks
 */
case class ProgressInfo(successTasks: Int, failedTasks: Int) {
  /**
   * return the string representation of the progress info
   */
  override def toString() = {
    s"Tasks Succeeded: $successTasks Failed: $failedTasks"
  }
}