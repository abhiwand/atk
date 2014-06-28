package com.intel.intelanalytics.engine.spark

import com.intel.intelanalytics.engine.StageProgressInfo

/**
 * Execute when receiving progress update for command
 */
trait CommandProgressUpdater {
  def updateProgress(commandId: Long, progress: List[Float], detailedProgress: List[StageProgressInfo])
}
