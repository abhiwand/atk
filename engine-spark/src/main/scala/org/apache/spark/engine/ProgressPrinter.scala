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

/**
 * Create for demo purpose. It is used to get progress from SparkProgressListener and print it out
 * TODO: remove it when progress report is exposed through rest api
 */
class ProgressPrinter(progressListener: SparkProgressListener) extends SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    printJobProgress()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    printJobProgress()
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    printJobProgress()
  }

  def printJobProgress() {
    val jobIds = progressListener.jobIdToStageIds.keys.toList.sorted
    println("PRINTING PROGRESS........................................................")
    for (id <- jobIds) {
      println("job: " + id + ", progress: " + progressListener.getProgress(id) + "%")
    }
    println("END.......................................................................")
  }
}
