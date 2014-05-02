package org.apache.spark.engine

import org.apache.spark.scheduler._
import scala.collection.mutable.{ListBuffer, HashSet, HashMap}
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.JobFailed
import org.apache.spark.scheduler.SparkListenerStageCompleted
import scala.Some
import org.apache.spark.scheduler.SparkListenerJobStart
import scala.concurrent._

class SparkProgressListener extends SparkListener {

  val jobIdToStageIds = new HashMap[Int, Array[Int]]
  val activeStages = HashSet[StageInfo]()
  val completedStages = ListBuffer[StageInfo]()
  val stageIdToTasksComplete = HashMap[Int, Int]()
  val stageIdToTasksFailed = HashMap[Int, Int]()
  var jobIdPromise: Promise[Int] = null

  def getJobId(): Future[Int] = {
    val p = promise[Int]
    val f = p.future
    jobIdPromise = p
    f
  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val parents = jobStart.job.finalStage.parents
    val parentsIds = parents.map(s => s.id)
    jobIdToStageIds(jobStart.job.jobId) = (parentsIds :+ jobStart.job.finalStage.id).toArray

    if(jobIdPromise != null) {
      jobIdPromise success jobStart.job.jobId
      jobIdPromise = null
    }
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

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {

    jobEnd match {
      case end: SparkListenerJobEnd =>
        end.jobResult match {
          case JobFailed(ex, Some(stage)) =>
            /* If two jobs share a stage we could get this failure message twice. So we first
            *  check whether we've already retired this stage. */
            val stageInfo = activeStages.filter(s => s.stageId == stage.id).headOption
            stageInfo.foreach {s =>
              activeStages -= s
            }
          case _ =>
        }
      case _ =>
    }
  }

  def getProgress(jobId: Int): Int = {

    val stageIds = jobIdToStageIds(jobId)
    val finishedStages = stageIds.count(i=> completedStages.filter(s => s.stageId == i).length > 0)
    val currentActiveStages = activeStages.filter(s => stageIds.contains(s.stageId))
    var progress = (100 * finishedStages) / stageIds.length

    currentActiveStages.foreach(currentActiveStage=> {
      val totalTaskForStage = currentActiveStage.numTasks
      val successCount = stageIdToTasksComplete.getOrElse(currentActiveStage.stageId, 0)
      progress += (100 * successCount / (totalTaskForStage * stageIds.length))
    })
    progress
  }
}


class TestListener(progressListener: SparkProgressListener) extends SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    printJobProgress()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    printJobProgress()
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    printJobProgress()
  }

  def printJobProgress() {
    val jobIds = progressListener.jobIdToStageIds.keys
    jobIds.foreach(id => println("job: " + id + ", progress: " + progressListener.getProgress(id)))
  }
}