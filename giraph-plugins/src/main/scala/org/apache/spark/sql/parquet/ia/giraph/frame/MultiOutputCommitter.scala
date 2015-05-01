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

package org.apache.spark.sql.parquet.ia.giraph.frame

import org.apache.hadoop.mapreduce.JobStatus.State
import org.apache.hadoop.mapreduce.{ JobContext, TaskAttemptContext, OutputCommitter }

/**
 * A Committer that wraps multiple Committers.
 *
 * This is helpful when output is more than one file.
 */
class MultiOutputCommitter(val committers: List[OutputCommitter]) extends OutputCommitter {

  override def needsTaskCommit(taskAttemptContext: TaskAttemptContext): Boolean = {
    committers.exists(committer => committer.needsTaskCommit(taskAttemptContext))
  }

  override def setupJob(jobContext: JobContext): Unit = {
    committers.foreach(_.setupJob(jobContext))
  }

  override def setupTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committers.foreach(_.setupTask(taskAttemptContext))
  }

  override def commitTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committers.foreach(_.commitTask(taskAttemptContext))
  }

  override def abortTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committers.foreach(_.abortTask(taskAttemptContext))
  }

  override def isRecoverySupported(jobContext: JobContext): Boolean = super.isRecoverySupported(jobContext)

  override def abortJob(jobContext: JobContext, state: State): Unit = {
    super.abortJob(jobContext, state)
    committers.foreach(_.abortJob(jobContext, state))
  }

  override def commitJob(jobContext: JobContext): Unit = {
    super.commitJob(jobContext)
    committers.foreach(_.commitJob(jobContext))
  }

  override def recoverTask(taskContext: TaskAttemptContext): Unit = {
    super.recoverTask(taskContext)
    committers.foreach(_.recoverTask(taskContext))
  }
}
