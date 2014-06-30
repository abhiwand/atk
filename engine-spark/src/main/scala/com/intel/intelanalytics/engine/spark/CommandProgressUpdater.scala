package com.intel.intelanalytics.engine.spark

import com.intel.intelanalytics.engine.ProgressInfo

/**
 * Execute when receiving progress update for command
 */
trait CommandProgressUpdater {
  def updateProgress(commandId: Long, progress: List[Float], detailedProgress: List[ProgressInfo])
}
