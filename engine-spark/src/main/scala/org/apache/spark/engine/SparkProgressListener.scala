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
import scala.collection.mutable.{ ListBuffer, HashSet, HashMap }
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.JobFailed
import org.apache.spark.scheduler.SparkListenerStageCompleted
import scala.Some
import org.apache.spark.scheduler.SparkListenerJobStart
import scala.concurrent._
import org.apache.spark.SparkContext
import scala.util.{Success, Try}

/**
 * Listens to progress on Spark Jobs.
 *
 * Requires access to classes private to org.apache.spark.engine
 */
class SparkProgressListener extends SparkListener {

  val jobIdToStageIds = new HashMap[Int, Array[Int]]
  val activeStages = HashSet[StageInfo]()
  val completedStages = ListBuffer[StageInfo]()
  val stageIdToTasksComplete = HashMap[Int, Int]()
  val stageIdToTasksFailed = HashMap[Int, Int]()
  var jobIdPromise: Promise[Int] = null
  val activeJobs = new HashMap[Int, ActiveJob]
  //  val jobIdToCommandId = HashMap[Int, Int]()

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
    activeJobs(jobStart.job.jobId) = jobStart.job


    //    val commandIdTry = Try {
    //      jobStart.job.properties.getProperty(SparkContext.SPARK_JOB_GROUP_ID).toInt
    //    }
    //
    //    commandIdTry match {
    //      case Success(r) => jobIdToCommandId(jobStart.job.jobId) = r
    //      case _ => { } // do nothing
    //    }

    if (jobIdPromise != null) {
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
            stageInfo.foreach { s =>
              activeStages -= s
            }
          case _ =>
        }
      case _ =>
    }
  }

  def getProgress(jobId: Int): Int = {

    val stageIds = jobIdToStageIds(jobId)
    val finishedStages = stageIds.count(i => completedStages.filter(s => s.stageId == i).length > 0)
    val currentActiveStages = activeStages.filter(s => stageIds.contains(s.stageId))
    var progress = (100 * finishedStages) / stageIds.length

    currentActiveStages.foreach(currentActiveStage => {
      val totalTaskForStage = currentActiveStage.numTasks
      val successCount = stageIdToTasksComplete.getOrElse(currentActiveStage.stageId, 0)
      progress += (100 * successCount / (totalTaskForStage * stageIds.length))
    })
    progress
  }
}

/**
 * Create for demo purpose. It is used to get progress from SparkProgressListener and print it out
 * TODO: remove it when progress report is exposed through rest api
 */
class ProgressPrinter(progressListener: SparkProgressListener) extends SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    printJobProgress()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    //    printJobProgress()
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    printJobProgress()
  }

  def printJobProgress() {
    println("PRINTING PROGRESS........................................................")
    for(jobId <- progressListener.activeJobs.keys.toList.sorted) {
      val job = progressListener.activeJobs(jobId)
      println( "command:" + job.properties.getProperty("command-id") +  ", job: " + job.jobId + ", progress: " + progressListener.getProgress(job.jobId) + "%")
    }
    println("END.......................................................................")
  }
}