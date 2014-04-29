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
import com.intel.intelanalytics.engine.spark.SparkProgressListener
import org.apache.spark.scheduler._
import org.specs2.mock.Mockito
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerJobStart

class ProgressListenerSpec extends Specification with Mockito  {

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

    jobStart.stageIds(0) = 1
    jobStart.stageIds(1) = 2
    jobStart.stageIds(2) = 3
    listener onJobStart jobStart
    listener
  }

  def createListener_two_jobs() : SparkProgressListener = {
    val listener = new SparkProgressListener()
    val stageIds = Array(1, 2, 3)

    val job1 = mock[ActiveJob]
    job1.jobId.returns(1)
    val jobStart1 = SparkListenerJobStart(job1, stageIds)

    jobStart1.stageIds(0) = 1
    jobStart1.stageIds(1) = 2
    jobStart1.stageIds(2) = 3
    listener onJobStart jobStart1


    val job2 = mock[ActiveJob]
    job2.jobId.returns(1)
    val jobStart2 = SparkListenerJobStart(job2, stageIds)

    jobStart2.stageIds(0) = 1
    jobStart2.stageIds(1) = 2
    jobStart2.stageIds(2) = 3
    jobStart2.stageIds(3) = 4
    listener onJobStart jobStart2

    listener
  }

  "initialize stages count" in {
    val listener = createListener_one_job
    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(10)
    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)
    listener.totalStages shouldEqual 3
    listener.finishedStages shouldEqual 0
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
    listener.finishedStages shouldEqual 1
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

    listener.finishedStages shouldEqual 2
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


    listener.finishedStages shouldEqual 3
    listener.getProgress(1) shouldEqual 100
  }

  "finish first task in first stage" in {
    val listener = createListener_one_job
    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(10)
    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)
    val taskEnd = mock[SparkListenerTaskEnd]
    val taskInfo = mock[TaskInfo]
    taskEnd.taskInfo.returns(taskInfo)
    taskInfo.successful.returns(true)
    listener.onTaskEnd(taskEnd)
    listener.getProgress(1) shouldEqual 3
  }

  "finish second task in second stage" in {
    val listener = createListener_one_job

    val stageInfo1 = mock[StageInfo]
    stageInfo1.stageId.returns(1)
    val completed1 = SparkListenerStageCompleted(stageInfo1)
    listener.onStageCompleted(completed1)

    listener.onStageCompleted(completed1)

    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(10)
    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)
    val taskEnd = mock[SparkListenerTaskEnd]
    val taskInfo = mock[TaskInfo]

    taskEnd.taskInfo.returns(taskInfo)
    taskInfo.successful.returns(true)
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
    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)
    val taskEnd = mock[SparkListenerTaskEnd]
    val taskInfo = mock[TaskInfo]

    taskEnd.taskInfo.returns(taskInfo)
    taskInfo.successful.returns(true)
    listener.onTaskEnd(taskEnd)
    listener.onTaskEnd(taskEnd)
    listener.onTaskEnd(taskEnd)

    listener.getProgress(1) shouldEqual 66
  }


  "finish all tasks in second stage" in {
    val listener = createListener_one_job
    val stageInfo1 = mock[StageInfo]
    stageInfo1.stageId.returns(1)
    val completed1 = SparkListenerStageCompleted(stageInfo1)
    listener.onStageCompleted(completed1)

    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(3)
    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)
    val taskEnd = mock[SparkListenerTaskEnd]
    val taskInfo = mock[TaskInfo]

    taskEnd.taskInfo.returns(taskInfo)
    taskInfo.successful.returns(true)
    listener.onTaskEnd(taskEnd)
    listener.onTaskEnd(taskEnd)
    listener.onTaskEnd(taskEnd)

    listener.getProgress(1) shouldEqual 66
    val stageInfo2 = mock[StageInfo]
    stageInfo2.stageId.returns(2)
    val completed2 = SparkListenerStageCompleted(stageInfo2)
    listener.onStageCompleted(completed2)
    listener.getProgress(1) shouldEqual 66
  }



}
