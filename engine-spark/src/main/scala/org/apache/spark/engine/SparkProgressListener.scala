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
import scala.collection.mutable.{ ListBuffer, HashMap, HashSet }
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerStageCompleted
import scala.Some
import org.apache.spark.scheduler.SparkListenerJobStart
import com.intel.intelanalytics.engine.spark.CommandProgressUpdater
import com.intel.intelanalytics.engine.{ ProgressInfo, TaskProgressInfo }

/**
 * Listens to progress on Spark Jobs.
 *
 * Requires access to classes private to org.apache.spark.engine
 */
object SparkProgressListener {
  var progressUpdater: CommandProgressUpdater = null
}

class SparkProgressListener(val progressUpdater: CommandProgressUpdater) extends SparkListener {

  val jobIdToStagesIds = new HashMap[Int, Array[Int]]
  val stageIdStageMapping = new HashMap[Int, Stage]
  val unfinishedStages = new HashMap[Int, StageInfo]()
  val completedStages = ListBuffer[Int]()
  val stageIdToTasksComplete = HashMap[Int, Int]()
  val stageIdToTasksFailed = HashMap[Int, Int]()
  val commandIdJobs = new HashMap[Long, List[ActiveJob]]
  val commandIdJobCount = new HashMap[Long, Int]()

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val stages = addStageAndAncestorStagesToCollection(jobStart.job.finalStage)

    val stageIds = stages.map(s => s.id)
    jobIdToStagesIds(jobStart.job.jobId) = stageIds.toArray
    stageIdStageMapping ++= stages.map(stage => (stage.id, stage))

    val job = jobStart.job

    if (hasCommandId(job)) {
      addToCommandIdJobs(job)

      //update initial progress to 0
      progressUpdater.updateProgress(job.properties.getProperty("command-id").toLong, List(ProgressInfo(0.00f, Some(TaskProgressInfo(0)))))
    }
  }

  /**
   * Specify the number of expected jobs initiated by the command
   * @param commandId command id
   * @param jobCount expected number of jobs
   */
  def setJobCountForCommand(commandId: Long, jobCount: Int) {
    commandIdJobCount(commandId) = jobCount
  }

  /**
   * Return all the ancestors stages and the stage itself
   * @param stage the stage to find all the ancestors
   */
  def addStageAndAncestorStagesToCollection(stage: Stage): Seq[Stage] = {
    val stageList = new HashSet[Stage]

    if (stage.parents != null) {
      stage.parents.foreach(s => {
        stageList ++= addStageAndAncestorStagesToCollection(s)
      })
    }

    stageList += stage
    stageList.toSeq
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    val stage = stageSubmitted.stage
    unfinishedStages(stage.stageId) = stage
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {

    val stageInfo = stageCompleted.stage
    unfinishedStages -= stageInfo.stageId
    completedStages += stageInfo.stageId

    //make sure all the parent stages are marked to complete
    val stage = stageIdStageMapping(stageInfo.stageId)
    val markToCompleteStages = addStageAndAncestorStagesToCollection(stage)
    for (stage <- markToCompleteStages) {
      if (!completedStages.contains(stage.id))
        completedStages += stage.id
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val sid = taskEnd.task.stageId
    taskEnd.taskInfo.successful match {
      case true => stageIdToTasksComplete(sid) = stageIdToTasksComplete.getOrElse(sid, 0) + 1
      case false => stageIdToTasksFailed(sid) = stageIdToTasksFailed.getOrElse(sid, 0) + 1
    }

    updateProgress(sid)
  }

  /* Calculate progress for the job */
  private def getProgress(jobId: Int): Float = {
    val totalStageIds = jobIdToStagesIds(jobId)
    val finished = totalStageIds.count(i => completedStages.contains(i))
    val unfinished = unfinishedStages.filter { case (id, _) => totalStageIds.contains(id) }.map { case (id, stage) => stage }

    val totalStageCount: Int = totalStageIds.length
    var progress: Float = (100 * finished.toFloat) / totalStageCount.toFloat

    unfinished.foreach(stage => {
      val totalTasksCount = stage.numTasks
      val successCount = stageIdToTasksComplete.getOrElse(stage.stageId, 0)
      progress += (100 * successCount.toFloat / (totalTasksCount * totalStageCount).toFloat)
    })

    BigDecimal(progress).setScale(2, BigDecimal.RoundingMode.DOWN).toFloat
  }

  /**
   * Return a detailed progress info about current job.
   */
  private def getDetailedProgress(jobId: Int): TaskProgressInfo = {
    val stageIds = jobIdToStagesIds(jobId)
    var totalFailed = 0
    for (stageId <- stageIds) {
      totalFailed += stageIdToTasksFailed.getOrElse(stageId, 0)
    }

    TaskProgressInfo(totalFailed)
  }

  /**
   * Calculate progress for the command
   */
  def getCommandProgress(commandId: Long): List[ProgressInfo] = {

    val jobList = commandIdJobs.getOrElse(commandId, throw new IllegalArgumentException(s"No such command: $commandId"))
    val expectedJobCount = commandIdJobCount.getOrElse(commandId, throw new IllegalArgumentException(s"No expected job count set for the command: $commandId"))

    var progress = 0f
    var retriedCounts = 0

    jobList.zip(1 to expectedJobCount).foreach {
      case (job, _) =>
        progress += getProgress(job.jobId)
        retriedCounts += getDetailedProgress(job.jobId).retries
    }

    val result = new ListBuffer[ProgressInfo]()
    result += ProgressInfo(progress / expectedJobCount.toFloat, Some(TaskProgressInfo(retriedCounts)))

    val unexpected = for {
      i <- expectedJobCount to (jobList.length - 1)
      job = jobList(i)
      progress = getProgress(job.jobId)
      taskInfo = getDetailedProgress(job.jobId)
    } yield ProgressInfo(progress, Some(taskInfo))

    result ++= unexpected
    result.toList
  }

  /**
   * Update the progress information and send it to progress updater
   */
  private def updateProgress(stageId: Int) {
    val jobIdStagePairOption = jobIdToStagesIds.find {
      case (_, stagesIds) =>
        stagesIds.contains(stageId)
    }

    jobIdStagePairOption match {
      case Some((jobId, _)) => {
        val fnGetListJobId = (jobs: List[ActiveJob]) => jobs.map(job => job.jobId)

        commandIdJobs.find { case (_, jobs) => fnGetListJobId(jobs).contains(jobId) }.
          foreach {
            case (commandId, _) => {
              val progressInfo = getCommandProgress(commandId)
              progressUpdater.updateProgress(commandId, progressInfo)
            }
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

