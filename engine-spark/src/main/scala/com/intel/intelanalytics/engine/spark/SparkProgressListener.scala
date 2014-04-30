package com.intel.intelanalytics.engine.spark

import org.apache.spark.scheduler._
import scala.collection.mutable.{ListBuffer, HashSet, HashMap}
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerJobStart


class SparkProgressListener extends SparkListener {

  val jobIdToStageIds = new HashMap[Int, Array[Int]]
  val activeStages = HashSet[StageInfo]()
  val completedStages = ListBuffer[StageInfo]()
  val failedStages = ListBuffer[StageInfo]()
  val stageIdToTasksComplete = HashMap[Int, Int]()
  val stageIdToTasksFailed = HashMap[Int, Int]()

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val parents = jobStart.job.finalStage.parents
    val parentsIds = parents.sortBy(_.id).map(s => s.id)
    jobIdToStageIds(jobStart.job.jobId) = (parentsIds :+ jobStart.job.finalStage.id).toArray
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    val stage = stageSubmitted.stage
    activeStages += stage
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {

    val stage = stageCompleted.stage
    activeStages -= stage
    completedStages += stage
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {

    val sid = taskEnd.task.stageId
    taskEnd.taskInfo.successful match {
      case true =>
        stageIdToTasksComplete(sid) = stageIdToTasksComplete.getOrElse(sid, 0) + 1
      case false =>

    }
  }

  def getProgress(jobId: Int): Int = {

    val stageIds = jobIdToStageIds(jobId)
    val finishedStages = stageIds.count(i=> completedStages.filter(s => s.stageId == i).length > 0)
    val currentActiveStage = activeStages.filter(s => stageIds.contains(s.stageId)).headOption
    var progress = (100 * finishedStages) / stageIds.length

    if (currentActiveStage != None) {
      val totalTaskForStage = currentActiveStage.get.numTasks
      val successCount = stageIdToTasksComplete.getOrElse(currentActiveStage.get.stageId, 0)
      progress += (100 * successCount / (totalTaskForStage * stageIds.length))
    }
    progress
  }

}