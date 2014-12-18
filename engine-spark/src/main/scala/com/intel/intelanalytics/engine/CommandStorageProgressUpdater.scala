package com.intel.intelanalytics.engine

import com.intel.intelanalytics.engine.spark.CommandProgressUpdater

class CommandStorageProgressUpdater(commandStorage: CommandStorage) extends CommandProgressUpdater {

  var lastUpdateTime = System.currentTimeMillis()

  /**
   * save the progress update
   * @param commandId id of the command
   * @param progressInfo list of progress for jobs initiated by the command
   */
  override def updateProgress(commandId: Long, progressInfo: List[ProgressInfo]): Unit = {
    val currentTime = System.currentTimeMillis()
    if (currentTime - lastUpdateTime > 1000) {
      lastUpdateTime = currentTime
      commandStorage.updateProgress(commandId, progressInfo)
    }
  }
}
