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
/*
* Unit test specs for Parser
*/
package com.intel.intelanalytics.engine

import org.specs2.mutable.Specification
import com.intel.intelanalytics.engine.spark.SparkProgressListener
import org.apache.spark.scheduler.SparkListenerJobStart
import org.specs2.mock.Mockito

class ProgressListenerSpec extends Specification with Mockito {

  def createListener() : SparkProgressListener = {
    val listener = new SparkProgressListener()
    val stageIds = Array(1, 2, 3)
    val jobStart = SparkListenerJobStart(null, stageIds)
    jobStart.stageIds(0) = 1
    jobStart.stageIds(1) = 2
    jobStart.stageIds(2) = 3
    listener onJobStart jobStart
    listener
  }

  "initialize stages count" in {
    val listener = createListener
    listener.totalStages shouldEqual 3
    listener.finishedStages shouldEqual 0
    listener.getProgress() shouldEqual 0
  }

  "finish first stage" in {
    val listener = createListener
    listener.onStageCompleted(null)
    listener.finishedStages shouldEqual 1
    listener.getProgress() shouldEqual 33
  }

  "finish second stage" in {
    val listener = createListener
    listener.onStageCompleted(null)
    listener.onStageCompleted(null)
    listener.finishedStages shouldEqual 2
    listener.getProgress() shouldEqual 66
  }

  "finish all stages" in {
    val listener = createListener
    listener.onStageCompleted(null)
    listener.onStageCompleted(null)
    listener.onStageCompleted(null)
    listener.finishedStages shouldEqual 3
    listener.getProgress() shouldEqual 100
  }

}
