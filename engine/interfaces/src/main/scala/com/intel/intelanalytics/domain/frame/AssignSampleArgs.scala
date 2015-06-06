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

package com.intel.intelanalytics.domain.frame

case class AssignSampleArgs(frame: FrameReference,
                            samplePercentages: List[Double],
                            sampleLabels: Option[List[String]] = None,
                            outputColumn: Option[String] = None,
                            randomSeed: Option[Int] = None) {
  require(frame != null, "AssignSample requires a non-null dataframe.")

  require(samplePercentages != null, "AssignSample requires that the percentages vector be non-null.")
  require(samplePercentages.length > 0, "AssignSample  requires that the percentages vector contain at least one value.")

  require(samplePercentages.forall(_ >= 0.0d), "AssignSample requires that all percentages be non-negative.")
  require(samplePercentages.forall(_ <= 1.0d), "AssignSample requires that all percentages be no more than 1.")

  def sumOfPercentages = samplePercentages.sum

  require(sumOfPercentages > 1.0d - 0.000000001,
    "AssignSample:  Sum of provided probabilities falls below one (" + sumOfPercentages + ")")
  require(sumOfPercentages < 1.0d + 0.000000001,
    "AssignSample:  Sum of provided probabilities exceeds one (" + sumOfPercentages + ")")

  def seed = randomSeed.getOrElse(0)
  def outputColumnName = outputColumn.getOrElse("sample_bin")

  def splitLabels: Array[String] = if (sampleLabels.isEmpty) {
    if (samplePercentages.length == 3) {
      Array("TR", "TE", "VA")
    }
    else {
      (0 to samplePercentages.length - 1).map(i => "Sample_" + i).toArray
    }
  }
  else {
    sampleLabels.get.toArray
  }
}
