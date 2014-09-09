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
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.TaskContext
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

    val jobStart = SparkListenerJobStart(job.jobId, stageIds)

    listener onJobStart jobStart
    listener
  }

  def createListener_two_jobs(commandId: Long, expectedJobs: Int = 2): SparkProgressListener = {
    val listener = new SparkProgressListener(new TestProgressUpdater(), commandId, expectedJobs)
    val stageIds = Array(1, 2, 3)

    val job1 = mock[ActiveJob]
    when(job1.jobId).thenReturn(1)

    val jobStart1 = SparkListenerJobStart(job1.jobId, Array(1, 2, 3))
    listener onJobStart jobStart1

    val job2 = mock[ActiveJob]
    when(job2.jobId).thenReturn(2)

    val jobStart2 = SparkListenerJobStart(job2.jobId, Array(4, 5, 6, 7))
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
    when(stageInfo.failureReason).thenReturn(None)
    listener.onStageCompleted(SparkListenerStageCompleted(stageInfo))
  }

  "get all stages" in {
    val listener = new SparkProgressListener(new TestProgressUpdater(), 1, 1)
    val job = mock[ActiveJob]
    when(job.jobId).thenReturn(1)

    val jobStart = SparkListenerJobStart(job.jobId, Array[Int](1, 2, 3, 4, 5), null)
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
    sendStageSubmittedToListener(listener, 2, 10)
    sendStageCompletedToListener(listener, 2)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(66.66f)
  }

  "finish all stages" in {
    val listener = createListener_one_job(1)

    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)
    sendStageCompletedToListener(listener, 2)
    sendStageSubmittedToListener(listener, 3, 10)
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
    listener.stageIdToTasksComplete(1) = listener.stageIdToTasksComplete.getOrElse(1, 0) + 1
    listener.getCommandProgress() shouldEqual List(ProgressInfo(3.33f, Some(TaskProgressInfo(0))))
  }

  "finish second task in second stage" in {
    val listener = createListener_one_job(1)

    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)
    listener.stageIdToTasksComplete(2) = listener.stageIdToTasksComplete.getOrElse(2, 0) + 2
    listener.getCommandProgress() shouldEqual List(ProgressInfo(40f, Some(TaskProgressInfo(0))))
  }
  //
  "finish second task in second stage, second task in third stage" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)
    listener.stageIdToTasksComplete(2) = listener.stageIdToTasksComplete.getOrElse(2, 0) + 2

    sendStageSubmittedToListener(listener, 3, 10)
    listener.stageIdToTasksComplete(3) = listener.stageIdToTasksComplete.getOrElse(3, 0) + 2
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(46.66f)
  }

  "finish all tasks in second stage" in {
    val listener: SparkProgressListener = finishAllTasksInSecondStage
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(66.66f)
  }

  private def finishAllTasksInSecondStage: SparkProgressListener = {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 3)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 3)
    listener.stageIdToTasksComplete(2) = listener.stageIdToTasksComplete.getOrElse(2, 0) + 3
    listener
  }
  //
  "finish all tasks in second stage-2" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 3)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 3)
    listener.stageIdToTasksComplete(2) = listener.stageIdToTasksComplete.getOrElse(2, 0) + 3
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

    listener.onJobEnd(jobEnd)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(0)
  }

  "failed at middle of stage" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 10)
    listener.stageIdToTasksComplete(1) = listener.stageIdToTasksComplete.getOrElse(1, 0) + 6

    listener.getCommandProgress().map(info => info.progress) shouldEqual List(20)
    val stage = mock[Stage]
    when(stage.id).thenReturn(1)

    listener.getCommandProgress().map(info => info.progress) shouldEqual List(20)
  }

  "get two progress info for a single command" in {
    val listener: SparkProgressListener = createListener_two_jobs(1)
    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)

    listener.stageIdToTasksComplete(2) = listener.stageIdToTasksComplete.getOrElse(2, 0) + 2
    listener.stageIdToTasksFailed(2) = listener.stageIdToTasksFailed.getOrElse(2, 0) + 1

    listener.getCommandProgress() shouldEqual List(ProgressInfo(20f, Some(TaskProgressInfo(1))))

    sendStageSubmittedToListener(listener, 4, 10)
    listener.stageIdToTasksFailed(4) = listener.stageIdToTasksFailed.getOrElse(4, 0) + 1

    listener.getCommandProgress() shouldEqual List(ProgressInfo(20f, Some(TaskProgressInfo(2))))
    listener.stageIdToTasksComplete(4) = listener.stageIdToTasksComplete.getOrElse(4, 0) + 1
    listener.getCommandProgress() shouldEqual List(ProgressInfo(21.25f, Some(TaskProgressInfo(2))))

    sendStageCompletedToListener(listener, 2)
    sendStageSubmittedToListener(listener, 3, 10)

    listener.stageIdToTasksComplete(3) = listener.stageIdToTasksComplete.getOrElse(3, 0) + 10
    sendStageCompletedToListener(listener, 3)
    listener.getCommandProgress().map(info => info.tasksInfo) shouldEqual List(Some(TaskProgressInfo(2)))
  }

  "get two progress info for a single command, expected number of jobs is 1 less than actual" in {
    val listener: SparkProgressListener = createListener_two_jobs(1, 1)

    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)

    listener.stageIdToTasksComplete(2) = listener.stageIdToTasksComplete.getOrElse(2, 0) + 2
    listener.stageIdToTasksFailed(2) = listener.stageIdToTasksFailed.getOrElse(2, 0) + 1

    listener.getCommandProgress() shouldEqual List(ProgressInfo(40f, Some(TaskProgressInfo(1))), ProgressInfo(0f, Some(TaskProgressInfo(0))))

    sendStageSubmittedToListener(listener, 4, 10)

    listener.stageIdToTasksFailed(4) = listener.stageIdToTasksFailed.getOrElse(4, 0) + 1
    listener.getCommandProgress() shouldEqual List(ProgressInfo(40f, Some(TaskProgressInfo(1))), ProgressInfo(0f, Some(TaskProgressInfo(1))))
    listener.stageIdToTasksComplete(4) = listener.stageIdToTasksComplete.getOrElse(4, 0) + 1
    listener.getCommandProgress() shouldEqual List(ProgressInfo(40f, Some(TaskProgressInfo(1))), ProgressInfo(2.5f, Some(TaskProgressInfo(1))))

    sendStageCompletedToListener(listener, 2)
    sendStageSubmittedToListener(listener, 3, 10)

    listener.stageIdToTasksComplete(3) = listener.stageIdToTasksComplete.getOrElse(3, 0) + 10
    sendStageCompletedToListener(listener, 3)

    listener.getCommandProgress() shouldEqual List(ProgressInfo(100f, Some(TaskProgressInfo(1))), ProgressInfo(2.5f, Some(TaskProgressInfo(1))))
  }
}
