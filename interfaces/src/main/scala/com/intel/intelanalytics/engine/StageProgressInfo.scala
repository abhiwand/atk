package com.intel.intelanalytics.engine

case class StageProgressInfo(currentStage: Int, totalStages: Int, finishedTasks: Int, totalTasks: Int) {
  override def toString() = {
    s"Step $currentStage/$totalStages Task $finishedTasks/$totalTasks"
  }
}