/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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
