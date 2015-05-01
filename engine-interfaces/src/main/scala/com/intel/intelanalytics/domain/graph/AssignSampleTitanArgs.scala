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

package com.intel.intelanalytics.domain.graph

/**
 * parameters needed to assign_sample to a titan graph
 * @param graph graph to be sampled
 * @param samplePercentages percentages to use
 * @param sampleLabels labels to use if omitted will use Sample# unless there are 3 groups then TR, TE, VA
 * @param outputProperty property name for label
 * @param randomSeed random seed value to randomize the split
 */
case class AssignSampleTitanArgs(graph: GraphReference,
                                 samplePercentages: List[Double],
                                 sampleLabels: Option[List[String]] = None,
                                 outputProperty: Option[String] = None,
                                 randomSeed: Option[Int] = None) {
  require(graph != null, "AssignSample requires a non-null graph.")

  require(samplePercentages != null, "AssignSample requires that the percentages vector be non-null.")
  require(samplePercentages.length > 0, "AssignSample  requires that the percentages vector contain at least one value.")

  require(samplePercentages.forall(_ >= 0), "AssignSample requires that all percentages be non-negative")
  require(samplePercentages.sum <= 1, "AssignSample requires that percentages sum to no more than 1")

  require(sampleLabels.isEmpty || sampleLabels.getOrElse(List()).size == samplePercentages.size,
    "AssignSample requires a label for each group")

  def getSampleLabels: List[String] = sampleLabels match {
    case Some(labels) => labels
    case None => {
      if (samplePercentages.length == 3) {
        List("TR", "TE", "VA")
      }
      else {
        (0 to samplePercentages.length - 1).map(i => s"Sample_$i").toList
      }
    }
  }

  def getOutputProperty: String = outputProperty.getOrElse("sample_bin")

  def getRandomSeed: Int = randomSeed.getOrElse(0)
}
