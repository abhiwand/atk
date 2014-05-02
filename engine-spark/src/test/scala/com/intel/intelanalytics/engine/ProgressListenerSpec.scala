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


import org.specs2.mutable.Specification

import org.apache.spark.scheduler._
import org.specs2.mock.Mockito
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.{TaskContext, Success}
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global




class ProgressListenerSpec extends Specification with Mockito  {


  class FakeTask(stageId: Int) extends Task[Int](stageId, 0) {
    override def runTask(context: TaskContext): Int = ???
  }

  def createListener_one_job() : SparkProgressListener = {
    val listener = new SparkProgressListener()
    val stageIds = Array(1, 2, 3)

    val job = mock[ActiveJob]
    job.jobId.returns(1)
    val finalStage1 = mock[Stage]
    finalStage1.id.returns(3)
    val parent1 = mock[Stage]
    parent1.id.returns(1)
    val parent2 = mock[Stage]
    parent2.id.returns(2)
    finalStage1.parents.returns(List(parent1, parent2))

    job.finalStage.returns(finalStage1)

    val jobStart = SparkListenerJobStart(job, stageIds)

    listener onJobStart jobStart
    listener
  }

  def createListener_two_jobs() : SparkProgressListener = {
    val listener = new SparkProgressListener()
    val stageIds = Array(1, 2, 3)

    val job1 = mock[ActiveJob]
    job1.jobId.returns(1)
    val finalStage1 = mock[Stage]
    finalStage1.id.returns(3)
    val parent1 = mock[Stage]
    parent1.id.returns(1)
    val parent2 = mock[Stage]
    parent2.id.returns(2)
    finalStage1.parents.returns(List(parent1, parent2))
    job1.finalStage.returns(finalStage1)


    val jobStart1 = SparkListenerJobStart(job1, stageIds)
    listener onJobStart jobStart1


    val job2 = mock[ActiveJob]
    job2.jobId.returns(2)


    val finalStage2 = mock[Stage]
    finalStage2.id.returns(7)
    val parent2_1 = mock[Stage]
    parent2_1.id.returns(4)
    val parent2_2 = mock[Stage]
    parent2_2.id.returns(5)
    val parent2_3 = mock[Stage]
    parent2_3.id.returns(6)

    finalStage2.parents.returns(List(parent2_1, parent2_2, parent2_3))
    job2.finalStage.returns(finalStage2)

    val jobStart2 = SparkListenerJobStart(job2, stageIds)
    listener onJobStart jobStart2

    listener
  }

  "initialize stages count" in {
    val listener = createListener_one_job
    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(10)
    stageInfo.stageId.returns(1)

    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)
    listener.getProgress(1) shouldEqual 0
  }

  "finish first stage" in {
    val listener = createListener_one_job
    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(10)
    stageInfo.stageId.returns(1)

    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)

    val completed = SparkListenerStageCompleted(stageInfo)

    listener.onStageCompleted(completed)
    listener.getProgress(1) shouldEqual 33
  }

  "finish second stage" in {
    val listener = createListener_one_job

    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(10)
    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)

    val stageInfo1 = mock[StageInfo]
    stageInfo1.stageId.returns(1)
    val completed1 = SparkListenerStageCompleted(stageInfo1)
    listener.onStageCompleted(completed1)

    val stageInfo2 = mock[StageInfo]
    stageInfo2.stageId.returns(2)
    val completed2 = SparkListenerStageCompleted(stageInfo2)
    listener.onStageCompleted(completed2)

    listener.getProgress(1) shouldEqual 66
  }

  "finish all stages" in {
    val listener = createListener_one_job

    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(10)
    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)

    val stageInfo1 = mock[StageInfo]
    stageInfo1.stageId.returns(1)
    val completed1 = SparkListenerStageCompleted(stageInfo1)
    listener.onStageCompleted(completed1)

    val stageInfo2 = mock[StageInfo]
    stageInfo2.stageId.returns(2)
    val completed2 = SparkListenerStageCompleted(stageInfo2)
    listener.onStageCompleted(completed2)

    val stageInfo3 = mock[StageInfo]
    stageInfo3.stageId.returns(3)
    val completed3 = SparkListenerStageCompleted(stageInfo3)
    listener.onStageCompleted(completed3)
    listener.getProgress(1) shouldEqual 100

    val jobEnd = mock[SparkListenerJobEnd]
    jobEnd.jobResult.returns(JobSucceeded)
    listener.onJobEnd(jobEnd)
    listener.getProgress(1) shouldEqual 100
  }

  "finish first task in first stage" in {
    val listener = createListener_one_job
    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(10)
    stageInfo.stageId.returns(1)
    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)
    val taskInfo = mock[TaskInfo]
    taskInfo.successful.returns(true)

    val task = new FakeTask(1)
    val taskEnd = SparkListenerTaskEnd(task, Success, taskInfo, null)
    listener.onTaskEnd(taskEnd)
    listener.getProgress(1) shouldEqual 3
  }

  "finish second task in second stage" in {
    val listener = createListener_one_job

    val stageInfo1 = mock[StageInfo]
    stageInfo1.stageId.returns(1)
    val completed1 = SparkListenerStageCompleted(stageInfo1)
    listener.onStageCompleted(completed1)

    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(10)
    stageInfo.stageId.returns(2)
    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)

    val taskInfo = mock[TaskInfo]
    taskInfo.successful.returns(true)

    val task = new FakeTask(2)
    val taskEnd = SparkListenerTaskEnd(task, Success, taskInfo, null)

    listener.onTaskEnd(taskEnd)
    listener.onTaskEnd(taskEnd)
    listener.getProgress(1) shouldEqual 39
  }

  "finish all tasks in second stage" in {
    val listener = createListener_one_job

    val stageInfo1 = mock[StageInfo]
    stageInfo1.stageId.returns(1)
    val completed1 = SparkListenerStageCompleted(stageInfo1)
    listener.onStageCompleted(completed1)

    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(3)
    stageInfo.stageId.returns(2)
    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)

    val taskInfo = mock[TaskInfo]
    taskInfo.successful.returns(true)

    val task = new FakeTask(2)
    val taskEnd = SparkListenerTaskEnd(task, Success, taskInfo, null)


    listener.onTaskEnd(taskEnd)
    listener.onTaskEnd(taskEnd)
    listener.onTaskEnd(taskEnd)

    listener.getProgress(1) shouldEqual 66
  }


  "finish all tasks in second stage-2" in {
    val listener = createListener_one_job
    val stageInfo1 = mock[StageInfo]
    stageInfo1.stageId.returns(1)
    val completed1 = SparkListenerStageCompleted(stageInfo1)
    listener.onStageCompleted(completed1)

    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(3)
    stageInfo.stageId.returns(2)
    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)
    val taskInfo = mock[TaskInfo]
    taskInfo.successful.returns(true)
    val task = new FakeTask(2)
    val taskEnd = SparkListenerTaskEnd(task, Success, taskInfo, null)


    listener.onTaskEnd(taskEnd)
    listener.onTaskEnd(taskEnd)
    listener.onTaskEnd(taskEnd)

    listener.getProgress(1) shouldEqual 66
    val completed2 = SparkListenerStageCompleted(stageInfo)
    listener.onStageCompleted(completed2)
    listener.getProgress(1) shouldEqual 66
  }

  "failed at first stage" in {
    val listener = createListener_one_job
    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(10)
    stageInfo.stageId.returns(1)

    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)

    val jobEnd = mock[SparkListenerJobEnd]
    val stage = mock[Stage]
    stage.id.returns(1)
    jobEnd.jobResult.returns(JobFailed(null, Some(stage)))

    listener.activeStages.find(s=> s.stageId == 1) shouldNotEqual None
    listener.onJobEnd(jobEnd)
    listener.getProgress(1) shouldEqual 0
    listener.activeStages.find(s=> s.stageId == 1) shouldEqual None
  }


  "job1: finish second task in second stage, job2: finish first task in first stage" in {

    val listener = createListener_two_jobs()

    val stageInfo1_1 = mock[StageInfo]
    stageInfo1_1.stageId.returns(1)

    val completed1 = SparkListenerStageCompleted(stageInfo1_1)
    listener.onStageCompleted(completed1)

    val stageInfo1_2 = mock[StageInfo]
    stageInfo1_2.stageId.returns(2)
    stageInfo1_2.numTasks.returns(10)

    val submitted1_2 = SparkListenerStageSubmitted(stageInfo1_2, null)
    listener.onStageSubmitted(submitted1_2)

    val taskInfo1 = mock[TaskInfo]
    val task1 = new FakeTask(2)
    val taskEnd1 = SparkListenerTaskEnd(task1, Success, taskInfo1, null)

    taskInfo1.successful.returns(true)

    listener.onTaskEnd(taskEnd1)
    listener.onTaskEnd(taskEnd1)
    listener.getProgress(1) shouldEqual 39


    val stageInfo2_1 = mock[StageInfo]
    stageInfo2_1.stageId.returns(4)
    stageInfo2_1.numTasks.returns(10)

    val submitted2_1 = SparkListenerStageSubmitted(stageInfo2_1, null)
    listener.onStageSubmitted(submitted2_1)

    val taskInfo2 = mock[TaskInfo]
    val task2 = new FakeTask(4)
    val taskEnd2 = SparkListenerTaskEnd(task2, Success, taskInfo2, null)

    taskInfo2.successful.returns(false)
    listener.getProgress(2) shouldEqual 0
    listener.onTaskEnd(taskEnd2)

    taskInfo2.successful.returns(true)
    listener.onTaskEnd(taskEnd2)
    listener.getProgress(2) shouldEqual 2
  }


  "get job id" in {
    var jobId = 0
    val listener = new SparkProgressListener()
    val stageIds = Array(1)
    val job = mock[ActiveJob]
    job.jobId.returns(1)
    val finalStage1 = mock[Stage]
    finalStage1.id.returns(1)
    finalStage1.parents.returns(List())
    job.finalStage.returns(finalStage1)
    val jobStart = SparkListenerJobStart(job, stageIds)

    val jobIdFuture = listener.getJobId()
    listener onJobStart jobStart
    import scala.concurrent.duration._
    jobId = Await.result(jobIdFuture, 10000 microseconds)
    jobId shouldEqual 1


  }
}
