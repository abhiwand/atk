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

package com.intel.taproot.analytics.domain.frame

import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation }

case class CategoricalSummaryArgs(@ArgDoc("Frame in reference to fetch categorical summary") frame: FrameReference,
                                  @ArgDoc("List of Categorical Column Input consisting of column, topk and/or threshold") columnInput: List[CategoricalColumnInput]) {

  require(frame != null, "frame is required but not provided")
  require(columnInput.length > 0, "Column Input must not be empty. Please provide at least a single Column Input")
}

case class CategoricalColumnInput(column: String, topK: Option[Int], threshold: Option[Double]) {
  require(!column.isEmpty && column != null, "Column name should not be empty or null")
  require(topK == None || topK.get > 0, "top_k input value should be greater than 0")
  require(threshold == None || threshold.get >= 0.0, "threshold should be greater than or equal to zero")
}

case class LevelData(level: String, frequency: Int, percentage: Double)

case class CategoricalSummaryOutput(column: String, levels: List[LevelData])

case class CategoricalSummaryReturn(categoricalSummary: List[CategoricalSummaryOutput])
