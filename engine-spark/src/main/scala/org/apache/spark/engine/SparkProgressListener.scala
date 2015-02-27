//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

import com.intel.intelanalytics.domain.command.Command
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.ui.jobs.JobProgressListener
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

class SparkProgressListener(val progressUpdater: CommandProgressUpdater, val command: Command, val jobCount: Int) extends JobProgressListener(new SparkConf(true)) {

  val jobIdToStagesIds = new HashMap[Int, Array[Int]]

  val jobs = new ListBuffer[Int]

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val stageIds = jobStart.stageIds
    val jobId = jobStart.jobId

    jobIdToStagesIds(jobId) = stageIds.toArray
    jobs += jobId
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    super.onTaskEnd(taskEnd)
    updateProgress()
  }

  /* Calculate progress for the job */
  private def getProgress(jobId: Int): Float = {
    val totalStageIds = jobIdToStagesIds(jobId)
    val completedStageIds = completedStages.map(stageInfo => stageInfo.stageId).toSet

    val finishedCount = totalStageIds.count(i => completedStageIds.contains(i))
    val runningStages = activeStages.filter { case (id, _) => totalStageIds.contains(id) }.map { case (id, stage) => stage }

    val totalStageCount: Int = totalStageIds.length
    var progress: Float = if (totalStageCount == 0) 100 else (100 * finishedCount.toFloat) / totalStageCount.toFloat

    runningStages.foreach(stage => {
      val totalTasksCount = stage.numTasks
      val successCount = {
        stageIdToData.get((stage.stageId, stage.attemptId)) match {
          case None => 0
          case taskInfo => taskInfo.get.numCompleteTasks
        }
      }
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
    val runningStages = activeStages.filter { case (id, _) => stageIds.contains(id) }.map { case (id, stage) => stage }

    runningStages.foreach(stage => {
      totalFailed += {
        stageIdToData.get((stage.stageId, stage.attemptId)) match {
          case None => 0
          case taskInfo => taskInfo.get.numFailedTasks
        }
      }
    })

    TaskProgressInfo(totalFailed)
  }

  /**
   * Calculate progress for the command
   */
  def getCommandProgress(): List[ProgressInfo] = {
    var progress = 0f
    var retriedCounts = 0

    jobs.zip(1 to jobCount).foreach {
      case (jobId, _) =>
        progress += getProgress(jobId)
        retriedCounts += getDetailedProgress(jobId).retries
    }

    val result = new ListBuffer[ProgressInfo]()
    result += ProgressInfo(progress / jobCount.toFloat, Some(TaskProgressInfo(retriedCounts)))

    val unexpected = for {
      i <- jobCount to (jobs.length - 1)
      jobId = jobs(i)
      progress = getProgress(jobId)
      taskInfo = getDetailedProgress(jobId)
    } yield ProgressInfo(progress, Some(taskInfo))

    result ++= unexpected
    val allProgress: List[ProgressInfo] = result.toList

    /**
     * If there are multiple progress, mark every one to 100 except the last one
     */
    if (allProgress.length >= 2) {
      allProgress.zipWithIndex.map {
        case (value, index) => {
          if (index == result.length - 1) {
            ProgressInfo(value.progress, value.tasksInfo)
          }
          else {
            ProgressInfo(100, value.tasksInfo)
          }
        }
      }
    }
    else {
      allProgress
    }
  }

  /**
   * Update the progress information and send it to progress updater
   */
  private def updateProgress() {
    val progressInfo = getCommandProgress()
    progressUpdater.updateProgress(command.id, progressInfo)
  }
}
