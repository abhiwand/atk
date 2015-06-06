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

/**
 * Arguments needed to compute a histogram from a column
 *
 * @param frame frame containing the column to be computed
 * @param columnName name of column to be evalueated
 * @param numBins number of bins in histogram [Optional] if None Square-root choice will be used (ie math.floor(math.sqrt(frame.row_count))
 * @param weightColumnName of column containing weights [Optional] if None all observations are weighted equally
 * @param binType the type of binning algorithm to use: "equalwidth", "equaldepth" defaults to "equalwidth"
 */
case class HistogramArgs(frame: FrameReference, columnName: String, numBins: Option[Int], weightColumnName: Option[String], binType: Option[String] = Some("equalwidth")) {
  require(binType.isEmpty || binType == Some("equalwidth") || binType == Some("equaldepth"), "bin type must be 'equalwidth' or 'equaldepth', not " + binType)
  if (numBins.isDefined)
    require(numBins.get > 0, "the number of bins must be greater than 0")
}
