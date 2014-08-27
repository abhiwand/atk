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

package org.apache.spark.engine.spark

import org.scalatest.{ Matchers, WordSpec }

import org.apache.spark.scheduler._
import org.mockito.Mockito._
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.{ TaskContext, Success }
import com.intel.intelanalytics.engine.spark.CommandProgressUpdater
import org.apache.spark.engine.SparkProgressListener
import com.intel.intelanalytics.engine.{ ProgressInfo, TaskProgressInfo }
import org.scalatest.mock.MockitoSugar

class ProgressListenerTest extends WordSpec with Matchers with MockitoSugar {

  class FakeTask(stageId: Int) extends Task[Int](stageId, 0) {
    override def runTask(context: TaskContext): Int = ???
  }

  class TestProgressUpdater extends CommandProgressUpdater {

    val commandProgress = scala.collection.mutable.Map[Long, List[Float]]()

    override def updateProgress(commandId: Long, progress: List[ProgressInfo]): Unit = {
      commandProgress(commandId) = progress.map(info => info.progress)
    }
  }

  def createListener_one_job(commandId: Long): SparkProgressListener = {
    val listener = new SparkProgressListener(new TestProgressUpdater(), commandId.toLong, 1)

    val stageIds = Array(1, 2, 3)

    val job = mock[ActiveJob]
    when(job.jobId).thenReturn(1)
    val finalStage1 = mock[Stage]
    when(finalStage1.id).thenReturn(3)
    val parent1 = mock[Stage]
    when(parent1.id).thenReturn(1)
    val parent2 = mock[Stage]
    when(parent2.id).thenReturn(2)
    when(parent2.parents).thenReturn(List(parent1))

    when(finalStage1.parents).thenReturn(List(parent1, parent2))

    when(job.finalStage).thenReturn(finalStage1)

    val jobStart = SparkListenerJobStart(job, stageIds)

    listener onJobStart jobStart
    listener
  }

  def createListener_two_jobs(commandId: Long, expectedJobs: Int = 2): SparkProgressListener = {
    val listener = new SparkProgressListener(new TestProgressUpdater(), commandId, expectedJobs)
    val stageIds = Array(1, 2, 3)

    val job1 = mock[ActiveJob]
    when(job1.jobId).thenReturn(1)
    val finalStage1 = mock[Stage]
    when(finalStage1.id).thenReturn(3)
    val parent1 = mock[Stage]
    when(parent1.id).thenReturn(1)
    val parent2 = mock[Stage]
    when(parent2.id).thenReturn(2)
    when(finalStage1.parents).thenReturn(List(parent1, parent2))
    when(job1.finalStage).thenReturn(finalStage1)

    val jobStart1 = SparkListenerJobStart(job1, stageIds)
    listener onJobStart jobStart1

    val job2 = mock[ActiveJob]
    when(job2.jobId).thenReturn(2)

    val finalStage2 = mock[Stage]
    when(finalStage2.id).thenReturn(7)
    val parent2_1 = mock[Stage]
    when(parent2_1.id).thenReturn(4)
    val parent2_2 = mock[Stage]
    when(parent2_2.id).thenReturn(5)
    val parent2_3 = mock[Stage]
    when(parent2_3.id).thenReturn(6)

    when(finalStage2.parents).thenReturn(List(parent2_1, parent2_2, parent2_3))
    when(job2.finalStage).thenReturn(finalStage2)

    val jobStart2 = SparkListenerJobStart(job2, stageIds)
    listener onJobStart jobStart2

    listener
  }

  private def sendStageSubmittedToListener(listener: SparkProgressListener, stageId: Int, numTasks: Int) {
    val stageInfo = mock[StageInfo]
    when(stageInfo.numTasks).thenReturn(numTasks)
    when(stageInfo.stageId).thenReturn(stageId)

    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)
  }

  private def sendStageCompletedToListener(listener: SparkProgressListener, stageId: Int) {
    val stageInfo = mock[StageInfo]
    when(stageInfo.stageId).thenReturn(stageId)
    listener.onStageCompleted(SparkListenerStageCompleted(stageInfo))
  }

  private def sendTaskEndToListener(listener: SparkProgressListener, stageId: Int, numOfTimes: Int, success: Boolean) {
    val taskInfo = mock[TaskInfo]
    val task = new FakeTask(stageId)
    val taskEnd = SparkListenerTaskEnd(task, Success, taskInfo, null)

    when(taskInfo.successful).thenReturn(success)

    for (i <- 1 to numOfTimes) {
      listener.onTaskEnd(taskEnd)
    }
  }

  "get all stages" in {
    val listener = new SparkProgressListener(new TestProgressUpdater(), 1, 1)
    val job = mock[ActiveJob]
    when(job.jobId).thenReturn(1)

    val finalStage1 = mock[Stage]
    when(finalStage1.id).thenReturn(3)
    when(job.finalStage).thenReturn(finalStage1)

    val parent1 = mock[Stage]
    when(parent1.id).thenReturn(1)
    val parent2 = mock[Stage]
    when(parent2.id).thenReturn(2)
    when(finalStage1.parents).thenReturn(List(parent1, parent2))

    val parent1_1 = mock[Stage]
    when(parent1_1.id).thenReturn(4)
    val parent1_2 = mock[Stage]
    when(parent1_2.id).thenReturn(5)

    when(parent1.parents).thenReturn(List(parent1_1, parent1_2))

    val jobStart = SparkListenerJobStart(job, Array())
    listener onJobStart jobStart

    listener.jobIdToStagesIds(1).toList.sorted shouldEqual List(1, 2, 3, 4, 5)
  }

  "initialize stages count" in {
    val listener = createListener_one_job(1)

    sendStageSubmittedToListener(listener, 1, 10)

    listener.getCommandProgress() shouldEqual List(ProgressInfo(0, Some(TaskProgressInfo(0))))
  }

  "finish first stage" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)

    listener.getCommandProgress().map(info => info.progress) shouldEqual List(33.33f)
  }

  "finish second stage" in {
    val listener = createListener_one_job(1)

    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(33.33f)
    sendStageCompletedToListener(listener, 2)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(66.66f)
  }

  "finish all stages" in {
    val listener = createListener_one_job(1)

    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    sendStageCompletedToListener(listener, 2)
    sendStageCompletedToListener(listener, 3)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(100f)

    val jobEnd = mock[SparkListenerJobEnd]
    when(jobEnd.jobResult).thenReturn(JobSucceeded)
    listener.onJobEnd(jobEnd)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(100)
  }

  "finish first task in first stage" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 10)
    sendTaskEndToListener(listener, 1, 1, true)
    listener.getCommandProgress() shouldEqual List(ProgressInfo(3.33f, Some(TaskProgressInfo(0))))
  }

  "finish second task in second stage" in {
    val listener = createListener_one_job(1)

    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)
    sendTaskEndToListener(listener, 2, 2, true)

    listener.getCommandProgress() shouldEqual List(ProgressInfo(40f, Some(TaskProgressInfo(0))))
  }

  "finish second task in second stage, second task in third stage" in {
    val listener = createListener_one_job(1)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)
    sendTaskEndToListener(listener, 2, 2, true)

    sendStageSubmittedToListener(listener, 3, 10)
    sendTaskEndToListener(listener, 3, 2, true)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(46.66f)
  }

  "finish all tasks in second stage" in {
    val listener: SparkProgressListener = finishAllTasksInSecondStage
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(66.66f)
  }

  private def finishAllTasksInSecondStage: SparkProgressListener = {
    val listener = createListener_one_job(1)

    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 3)
    sendTaskEndToListener(listener, 2, 3, true)
    listener
  }

  "finish all tasks in second stage-2" in {
    val listener = createListener_one_job(1)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 3)
    sendTaskEndToListener(listener, 2, 3, true)

    listener.getCommandProgress().map(info => info.progress) shouldEqual List(66.66f)
    sendStageCompletedToListener(listener, 2)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(66.66f)
  }

  "failed at first stage" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 10)

    val jobEnd = mock[SparkListenerJobEnd]
    val stage = mock[Stage]
    when(stage.id).thenReturn(1)
    when(jobEnd.jobResult).thenReturn(JobFailed(null, Some(stage)))

    assert(listener.unfinishedStages.get(1) != None)
    listener.onJobEnd(jobEnd)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(0)
  }

  "failed at middle of stage" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 10)
    sendTaskEndToListener(listener, 1, 6, true)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(20)

    val jobEnd = mock[SparkListenerJobEnd]
    val stage = mock[Stage]
    when(stage.id).thenReturn(1)
    when(jobEnd.jobResult).thenReturn(JobFailed(null, Some(stage)))

    assert(listener.unfinishedStages.get(1) != None)
    listener.onJobEnd(jobEnd)

    //send second time, make sure no exception thrown
    listener.onJobEnd(jobEnd)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(20)
  }

  "save progress on stage complete" in {
    val listener: SparkProgressListener = finishAllTasksInSecondStage
    val updater = listener.progressUpdater.asInstanceOf[TestProgressUpdater]
    updater.commandProgress(1) shouldEqual List(66.66f)
  }

  "get two progress info for a single command" in {
    val listener: SparkProgressListener = createListener_two_jobs(1)

    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)

    sendTaskEndToListener(listener, 2, 2, true)
    sendTaskEndToListener(listener, 2, 1, false)

    listener.getCommandProgress() shouldEqual List(ProgressInfo(20f, Some(TaskProgressInfo(1))))

    sendStageSubmittedToListener(listener, 4, 10)

    sendTaskEndToListener(listener, 4, 1, false)
    listener.getCommandProgress() shouldEqual List(ProgressInfo(20f, Some(TaskProgressInfo(2))))
    sendTaskEndToListener(listener, 4, 1, true)
    listener.getCommandProgress() shouldEqual List(ProgressInfo(21.25f, Some(TaskProgressInfo(2))))

    sendStageCompletedToListener(listener, 2)
    sendStageSubmittedToListener(listener, 3, 10)

    sendTaskEndToListener(listener, 3, 10, true)
    sendStageCompletedToListener(listener, 3)
    listener.getCommandProgress().map(info => info.tasksInfo) shouldEqual List(Some(TaskProgressInfo(2)))

    //set expected jobs to 1, should see 2 progress since one is expected and the other one is not.
    //    listener.setJobCountForCommand(1, 1)
    //    listener.getCommandProgress(1) shouldEqual List(ProgressInfo(100f, Some(TaskProgressInfo(1))), ProgressInfo(2.5f, Some(TaskProgressInfo(1))))
  }

  "get two progress info for a single command, expected number of jobs is 1 less than actual" in {
    val listener: SparkProgressListener = createListener_two_jobs(1, 1)

    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)

    sendTaskEndToListener(listener, 2, 2, true)
    sendTaskEndToListener(listener, 2, 1, false)

    listener.getCommandProgress() shouldEqual List(ProgressInfo(40f, Some(TaskProgressInfo(1))), ProgressInfo(0f, Some(TaskProgressInfo(0))))

    sendStageSubmittedToListener(listener, 4, 10)

    sendTaskEndToListener(listener, 4, 1, false)
    listener.getCommandProgress() shouldEqual List(ProgressInfo(40f, Some(TaskProgressInfo(1))), ProgressInfo(0f, Some(TaskProgressInfo(1))))
    sendTaskEndToListener(listener, 4, 1, true)
    listener.getCommandProgress() shouldEqual List(ProgressInfo(40f, Some(TaskProgressInfo(1))), ProgressInfo(2.5f, Some(TaskProgressInfo(1))))

    sendStageCompletedToListener(listener, 2)
    sendStageSubmittedToListener(listener, 3, 10)

    sendTaskEndToListener(listener, 3, 10, true)
    sendStageCompletedToListener(listener, 3)

    listener.getCommandProgress() shouldEqual List(ProgressInfo(100f, Some(TaskProgressInfo(1))), ProgressInfo(2.5f, Some(TaskProgressInfo(1))))
  }

  "mark parent stage to complete when child stage is starting" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 2, 10)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(0f)
    sendTaskEndToListener(listener, 2, 3, false)
    sendTaskEndToListener(listener, 2, 10, true)
    sendStageCompletedToListener(listener, 2)
    listener.getCommandProgress() shouldEqual List(ProgressInfo(66.66f, Some(TaskProgressInfo(3))))
  }

}