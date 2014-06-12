package com.intel.intelanalytics.engine.spark

/**
 * Execute when receiving progress update for command
 */
trait CommandProgressUpdater {
  def updateProgress(commandId: Long, progress: List[Int])
}
