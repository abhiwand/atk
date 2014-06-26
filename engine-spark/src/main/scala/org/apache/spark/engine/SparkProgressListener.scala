//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package org.apache.spark.engine

import org.apache.spark.scheduler._
import scala.collection.mutable.{ ListBuffer, LinkedHashSet, HashMap }
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.JobFailed
import org.apache.spark.scheduler.SparkListenerStageCompleted
import scala.Some
import org.apache.spark.scheduler.SparkListenerJobStart
import scala.concurrent._
import org.apache.spark.SparkContext
import scala.util.{ Success, Try }
import com.intel.intelanalytics.engine.spark.CommandProgressUpdater
import scala.collection.mutable

/**
 * Listens to progress on Spark Jobs.
 *
 * Requires access to classes private to org.apache.spark.engine
 */
object SparkProgressListener {
  var progressUpdater: CommandProgressUpdater = null
}

class SparkProgressListener(val progressUpdater: CommandProgressUpdater) extends SparkListener {

  val jobIdToStageIds = new HashMap[Int, Array[Int]]
  val activeStages = LinkedHashSet[StageInfo]()
  val completedStages = ListBuffer[StageInfo]()
  val stageIdToTasksComplete = HashMap[Int, Int]()
  val stageIdToTasksFailed = HashMap[Int, Int]()
  val commandIdJobs = new HashMap[Long, List[ActiveJob]]

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val parents = jobStart.job.finalStage.parents
    val parentsIds = parents.map(s => s.id)
    jobIdToStageIds(jobStart.job.jobId) = (parentsIds :+ jobStart.job.finalStage.id).toArray

    val job = jobStart.job

    if (hasCommandId(job)) {
      addToCommandIdJobs(job)

      //update initial progress to 0
      progressUpdater.updateProgress(job.properties.getProperty("command-id").toLong, List(0.00f), "")
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
      case true => {
        stageIdToTasksComplete(sid) = stageIdToTasksComplete.getOrElse(sid, 0) + 1
        updateProgress(sid)
      }
      case false => stageIdToTasksFailed(sid) = stageIdToTasksFailed.getOrElse(sid, 0) + 1
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
            stageInfo.foreach { s =>
              activeStages -= s
            }
          case _ =>
        }
      case _ =>
    }
  }

  /* calculate progress for the job */
  private def getProgress(jobId: Int): Float = {
    val stageIds = jobIdToStageIds(jobId)
    val finishedStages = stageIds.count(i => completedStages.filter(s => s.stageId == i).length > 0)
    val currentActiveStages = activeStages.filter(s => stageIds.contains(s.stageId))
    var progress: Float = (100 * finishedStages.toFloat) / stageIds.length.toFloat

    currentActiveStages.foreach(currentActiveStage => {
      val totalTaskForStage = currentActiveStage.numTasks
      val successCount = stageIdToTasksComplete.getOrElse(currentActiveStage.stageId, 0)
      progress += (100 * successCount.toFloat / (totalTaskForStage * stageIds.length).toFloat)
    })

    BigDecimal(progress).setScale(2, BigDecimal.RoundingMode.DOWN).toFloat
  }

  /**
   * return a message about current job progress.
   * eg. step 2/5 tasks 500/1000
   */
  private def getProgressMessage(jobId: Int): String = {
    val stageIds = jobIdToStageIds(jobId)
    val currentActiveStages = activeStages.filter(s => stageIds.contains(s.stageId))
    val message: mutable.ListBuffer[String] = mutable.ListBuffer()

    for (activeStage <- currentActiveStages) {
      val currentStep = stageIds.indexOf(activeStage.stageId) + 1
      val totalStages = stageIds.length
      val totalTaskForStage = activeStage.numTasks
      val successCount = stageIdToTasksComplete.getOrElse(activeStage.stageId, 0)
      message += s"step $currentStep/$totalStages task $successCount/$totalTaskForStage"
    }
    message.mkString(", ")
  }

  /**
   * calculate progress for the command
   */
  def getCommandProgress(commandId: Long): List[Float] = {
    val jobList = commandIdJobs.getOrElse(commandId, throw new IllegalArgumentException(s"No such command: $commandId"))
    jobList.map(job => getProgress(job.jobId))
  }

  /**
   * return message about the command's progress
   */
  def getCommandProgressMessage(commandId: Long): String = {
    val jobList = commandIdJobs.getOrElse(commandId, throw new IllegalArgumentException(s"No such command: $commandId"))
    getProgressMessage(jobList.last.jobId)
  }

  /**
   * update the progress information and send it to progress updater
   */
  private def updateProgress(stageId: Int) {
    val jobIdStageIdPairOption = jobIdToStageIds.find(e => e._2.contains(stageId))
    jobIdStageIdPairOption match {
      case Some(r) => {
        val jobId = r._1

        val fnGetListJobId = (jobs: List[ActiveJob]) => jobs.map(job => job.jobId)

        val commandIdJobOption = commandIdJobs.find(e => fnGetListJobId(e._2).contains(jobId))
        commandIdJobOption match {
          case Some(c) => {
            val commandId = c._1
            val progress = getCommandProgress(commandId)
            val progressMessage = getCommandProgressMessage(commandId)
            progressUpdater.updateProgress(commandId, progress, progressMessage)
          }

          case None =>
        }
      }

      case _ => println(s"missing command id for stage $stageId")
    }
  }

  /**
   * Some jobs don't have command id, explain
   */
  def hasCommandId(job: ActiveJob): Boolean = {
    job.properties != null && job.properties.containsKey("command-id")
  }

  /**
   * Keep track of id's to jobs in a Map
   */
  def addToCommandIdJobs(job: ActiveJob) {
    val id = job.properties.getProperty("command-id").toLong
    if (!commandIdJobs.contains(id))
      commandIdJobs(id) = List(job)
    else
      commandIdJobs(id) = commandIdJobs(id) ++ List(job)
  }
}

