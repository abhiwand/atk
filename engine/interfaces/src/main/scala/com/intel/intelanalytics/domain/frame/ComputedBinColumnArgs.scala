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
 * Represents the required parameters for bin_equal_width or bin_equal_depth
 *
 * @param frame identifier for the input dataframe
 * @param columnName name of the column to bin
 * @param numBins number of bins to partition column into if omitted the system will use the Square-root choice `math.floor(math.sqrt(frame.row_count))`
 * @param binColumnName name for the created binned column
 */
case class ComputedBinColumnArgs(frame: FrameReference, columnName: String, numBins: Option[Int], binColumnName: Option[String]) {
  require(frame != null, "frame is required")
  require(columnName != null, "column name is required")
  require(numBins.isEmpty || numBins.getOrElse(-1) > 0, "at least one bin is required")
}
