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

import org.specs2.mutable.Specification

import org.apache.spark.scheduler._
import org.specs2.mock.Mockito
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.{ TaskContext, Success }
import com.intel.intelanalytics.engine.spark.CommandProgressUpdater
import org.apache.spark.engine.SparkProgressListener
import java.util.Properties
import com.intel.intelanalytics.engine.ProgressInfo

class ProgressListenerSpec extends Specification with Mockito {

  class FakeTask(stageId: Int) extends Task[Int](stageId, 0) {
    override def runTask(context: TaskContext): Int = ???
  }

  class TestProgressUpdater extends CommandProgressUpdater {

    val commandProgress = scala.collection.mutable.Map[Long, List[Float]]()

    override def updateProgress(commandId: Long, progress: List[Float], detailedProgress: List[ProgressInfo]): Unit = { commandProgress(commandId) = progress }
  }

  def createListener_one_job(commandId: String): SparkProgressListener = {
    val listener = new SparkProgressListener(new TestProgressUpdater())

    val stageIds = Array(1, 2, 3)

    val job = mock[ActiveJob]
    job.jobId.returns(1)
    val finalStage1 = mock[Stage]
    finalStage1.id.returns(3)
    val parent1 = mock[Stage]
    parent1.id.returns(1)
    val parent2 = mock[Stage]
    parent2.id.returns(2)
    parent2.parents.returns(List(parent1))

    finalStage1.parents.returns(List(parent1, parent2))
    val properties = mock[java.util.Properties]
    properties.getProperty("command-id").returns(commandId)
    properties.containsKey("command-id").returns(true)
    job.properties.returns(properties)

    job.finalStage.returns(finalStage1)

    val jobStart = SparkListenerJobStart(job, stageIds)

    listener onJobStart jobStart
    listener
  }

  def createListener_two_jobs_two_commands(firstCommandId: String, secondCommandId: String): SparkProgressListener = {

    createListener_two_jobs(firstCommandId, secondCommandId)
  }

  def createListener_two_jobs(firstCommandId: String, secondCommandId: String): SparkProgressListener = {
    val listener = new SparkProgressListener(new TestProgressUpdater())
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

    val properties1 = mock[Properties]
    properties1.getProperty("command-id").returns(firstCommandId)
    properties1.containsKey("command-id").returns(true)
    job1.properties.returns(properties1)

    val jobStart1 = SparkListenerJobStart(job1, stageIds)
    listener onJobStart jobStart1

    val job2 = mock[ActiveJob]
    job2.jobId.returns(2)

    val properties2 = mock[Properties]

    properties2.getProperty("command-id").returns(secondCommandId)
    properties2.containsKey("command-id").returns(true)
    job2.properties.returns(properties2)

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

  def createListener_two_jobs_one_command(commandId: String): SparkProgressListener = {

    createListener_two_jobs(commandId, commandId)
  }

  private def sendStageSubmittedToListener(listener: SparkProgressListener, stageId: Int, numTasks: Int) {
    val stageInfo = mock[StageInfo]
    stageInfo.numTasks.returns(numTasks)
    stageInfo.stageId.returns(stageId)

    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)
  }

  private def sendStageCompletedToListener(listener: SparkProgressListener, stageId: Int) {
    val stageInfo = mock[StageInfo]
    stageInfo.stageId.returns(stageId)
    listener.onStageCompleted(SparkListenerStageCompleted(stageInfo))
  }

  private def sendTaskEndToListener(listener: SparkProgressListener, stageId: Int, numOfTimes: Int, success: Boolean) {
    val taskInfo = mock[TaskInfo]
    val task = new FakeTask(stageId)
    val taskEnd = SparkListenerTaskEnd(task, Success, taskInfo, null)

    taskInfo.successful.returns(success)

    for (i <- 1 to numOfTimes) {
      listener.onTaskEnd(taskEnd)
    }
  }

  "get all stages" in {
    val listener = new SparkProgressListener(new TestProgressUpdater())
    val job = mock[ActiveJob]
    job.jobId.returns(1)

    val finalStage1 = mock[Stage]
    finalStage1.id.returns(3)
    job.finalStage.returns(finalStage1)

    val parent1 = mock[Stage]
    parent1.id.returns(1)
    val parent2 = mock[Stage]
    parent2.id.returns(2)
    finalStage1.parents.returns(List(parent1, parent2))

    val parent1_1 = mock[Stage]
    parent1_1.id.returns(4)
    val parent1_2 = mock[Stage]
    parent1_2.id.returns(5)

    parent1.parents.returns(List(parent1_1, parent1_2))

    val jobStart = SparkListenerJobStart(job, Array())
    listener onJobStart jobStart

    listener.jobIdToStagesIds(1).toList.sorted shouldEqual List(1, 2, 3, 4, 5)
  }

  "initialize stages count" in {
    val listener = createListener_one_job("1")

    sendStageSubmittedToListener(listener, 1, 10)

    listener.getCommandProgress(1) shouldEqual List(0)
    listener.getDetailedCommandProgress(1) shouldEqual List()
  }

  "finish first stage" in {
    val listener = createListener_one_job("1")
    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)

    listener.getCommandProgress(1) shouldEqual List(33.33f)
  }

  "finish second stage" in {
    val listener = createListener_one_job("1")

    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    listener.getCommandProgress(1) shouldEqual List(33.33f)
    sendStageCompletedToListener(listener, 2)
    listener.getCommandProgress(1) shouldEqual List(66.66f)
  }

  "finish all stages" in {
    val listener = createListener_one_job("1")

    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    sendStageCompletedToListener(listener, 2)
    sendStageCompletedToListener(listener, 3)
    listener.getCommandProgress(1) shouldEqual List(100f)

    val jobEnd = mock[SparkListenerJobEnd]
    jobEnd.jobResult.returns(JobSucceeded)
    listener.onJobEnd(jobEnd)
    listener.getCommandProgress(1) shouldEqual List(100)
  }

  "finish first task in first stage" in {
    val listener = createListener_one_job("1")
    sendStageSubmittedToListener(listener, 1, 10)
    sendTaskEndToListener(listener, 1, 1, true)
    listener.getCommandProgress(1) shouldEqual List(3.33f)
    listener.getDetailedCommandProgress(1) shouldEqual List(ProgressInfo(1, 0))
  }

  "finish second task in second stage" in {
    val listener = createListener_one_job("1")

    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)
    sendTaskEndToListener(listener, 2, 2, true)

    listener.getCommandProgress(1) shouldEqual List(40f)
    listener.getDetailedCommandProgress(1) shouldEqual List(ProgressInfo(2, 0))
  }

  "finish second task in second stage, second task in third stage" in {
    val listener = createListener_one_job("1")
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)
    sendTaskEndToListener(listener, 2, 2, true)

    sendStageSubmittedToListener(listener, 3, 10)
    sendTaskEndToListener(listener, 3, 2, true)
    listener.getCommandProgress(1) shouldEqual List(46.66f)
  }

  "finish all tasks in second stage" in {
    val listener: SparkProgressListener = finishAllTasksInSecondStage
    listener.getCommandProgress(1) shouldEqual List(66.66f)
  }

  private def finishAllTasksInSecondStage: SparkProgressListener = {
    val listener = createListener_one_job("1")

    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 3)
    sendTaskEndToListener(listener, 2, 3, true)
    listener
  }

  "finish all tasks in second stage-2" in {
    val listener = createListener_one_job("1")
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 3)
    sendTaskEndToListener(listener, 2, 3, true)

    listener.getCommandProgress(1) shouldEqual List(66.66f)
    sendStageCompletedToListener(listener, 2)
    listener.getCommandProgress(1) shouldEqual List(66.66f)
  }

  "failed at first stage" in {
    val listener = createListener_one_job("1")
    sendStageSubmittedToListener(listener, 1, 10)

    val jobEnd = mock[SparkListenerJobEnd]
    val stage = mock[Stage]
    stage.id.returns(1)
    jobEnd.jobResult.returns(JobFailed(null, Some(stage)))

    listener.unfinishedStages.get(1) shouldNotEqual None
    listener.onJobEnd(jobEnd)
    listener.getCommandProgress(1) shouldEqual List(0)
  }

  "failed at middle of stage" in {
    val listener = createListener_one_job("1")
    sendStageSubmittedToListener(listener, 1, 10)
    sendTaskEndToListener(listener, 1, 6, true)
    listener.getCommandProgress(1) shouldEqual List(20)

    val jobEnd = mock[SparkListenerJobEnd]
    val stage = mock[Stage]
    stage.id.returns(1)
    jobEnd.jobResult.returns(JobFailed(null, Some(stage)))

    listener.unfinishedStages.get(1) shouldNotEqual None
    listener.onJobEnd(jobEnd)

    //send second time, make sure no exception thrown
    listener.onJobEnd(jobEnd)
    listener.getCommandProgress(1) shouldEqual List(20)
  }

  "job1: finish second task in second stage, job2: finish first task in first stage" in {

    val listener = createListener_two_jobs_two_commands("1", "2")
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)

    sendTaskEndToListener(listener, 2, 2, true)
    listener.getCommandProgress(1) shouldEqual List(40f)
    sendStageSubmittedToListener(listener, 4, 10)

    sendTaskEndToListener(listener, 4, 1, false)
    listener.getCommandProgress(2) shouldEqual List(0)
    sendTaskEndToListener(listener, 4, 1, true)
    listener.getCommandProgress(2) shouldEqual List(2.5f)
  }

  "query wrong command id should receive exception" in {
    val listener = createListener_two_jobs_two_commands("1", "2")
    listener.getCommandProgress(3) must throwAn[IllegalArgumentException]
  }

  "save progress on stage complete" in {
    val listener: SparkProgressListener = finishAllTasksInSecondStage
    val updater = listener.progressUpdater.asInstanceOf[TestProgressUpdater]
    updater.commandProgress(1) shouldEqual List(66.66f)
  }

  "get two progress info for a single command" in {
    val listener: SparkProgressListener = createListener_two_jobs_one_command("1")

    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)

    sendTaskEndToListener(listener, 2, 2, true)
    sendTaskEndToListener(listener, 2, 1, false)

    listener.getCommandProgress(1) shouldEqual List(40f, 0f)
    listener.getDetailedCommandProgress(1) shouldEqual List(ProgressInfo(2, 1), ProgressInfo(0, 0))

    sendStageSubmittedToListener(listener, 4, 10)

    sendTaskEndToListener(listener, 4, 1, false)
    listener.getCommandProgress(1) shouldEqual List(40f, 0f)
    sendTaskEndToListener(listener, 4, 1, true)
    listener.getCommandProgress(1) shouldEqual List(40f, 2.5f)

    listener.getDetailedCommandProgress(1) shouldEqual List(ProgressInfo(2, 1), ProgressInfo(1, 1))

    sendStageCompletedToListener(listener, 2)
    sendStageSubmittedToListener(listener, 3, 10)

    sendTaskEndToListener(listener, 3, 10, true)
    sendStageCompletedToListener(listener, 3)
    listener.getDetailedCommandProgress(1) shouldEqual List(ProgressInfo(12, 1), ProgressInfo(1, 1))
  }

  "mark parent stage to complete when child stage is starting" in {
    val listener = createListener_one_job("1")
    sendStageSubmittedToListener(listener, 2, 10)
    listener.getCommandProgress(1) shouldEqual List(0f)
    sendTaskEndToListener(listener, 2, 3, false)
    sendTaskEndToListener(listener, 2, 10, true)
    sendStageCompletedToListener(listener, 2)
    listener.getCommandProgress(1) shouldEqual List(66.66f)
    listener.getDetailedCommandProgress(1) shouldEqual List(ProgressInfo(10, 3))
  }
}
