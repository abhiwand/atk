package com.intel.intelanalytics.engine

case class ProgressInfo(successTasks: Int, failedTasks: Int) {
  override def toString() = {
    s"Tasks Succeeded: $successTasks Failed: $failedTasks"
  }
}