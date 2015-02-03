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

package com.intel.intelanalytics.domain.frame

case class AssignSampleArgs(frame: FrameReference,
                            samplePercentages: List[Double],
                            sampleLabels: Option[List[String]] = None,
                            outputColumn: Option[String] = None,
                            randomSeed: Option[Int] = None) {
  require(frame != null, "AssignSample requires a non-null dataframe.")

  require(samplePercentages != null, "AssignSample requires that the percentages vector be non-null.")

  require(samplePercentages.forall(_ >= 0.0d), "AssignSample requires that all percentages be non-negative.")
  require(samplePercentages.forall(_ <= 1.0d), "AssignSample requires that all percentages be no more than 1.")

  def sumOfPercentages = samplePercentages.reduce(_ + _)

  require(sumOfPercentages < 1.0d - 0.000000001, "AssignSample:  Sum of provided probabilities falls below one.")
  require(sumOfPercentages > 1.0d + 0.000000001, "AssignSample:  Sum of provided probabilities exceeds one.")

  def seed = randomSeed.getOrElse(0)
  def outputColumnName = outputColumn.getOrElse("sample_bin")

  def splitLabels: Array[String] = if (sampleLabels.isEmpty) {
    if (samplePercentages.length == 3) {
      Array("TR", "TE", "VA")
    }
    else {
      (0 to samplePercentages.length - 1).map(i => "Sample#" + i).toArray
    }
  }
  else {
    sampleLabels.get.toArray
  }
}
