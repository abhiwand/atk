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
 * Represents a BinColumn object
 *
 * @param frame identifier for the input dataframe
 * @param columnName name of the column to bin
 * @param cutoffs cutoff points of the bins
 * @param includeLowest if true the lowerbound of the bin will be inclusive while the upperbound is exclusive if false it is the opposite
 * @param strictBinning if true values smaller than the first bin or larger than the last bin will not be given a bin.
 *                      if false smaller vales will be in the first bin and larger values will be in the last
 * @param binColumnName name for the created binned column
 */
case class BinColumnArgs(frame: FrameReference, columnName: String, cutoffs: List[Double],
                         includeLowest: Option[Boolean], strictBinning: Option[Boolean], binColumnName: Option[String]) {
  require(frame != null, "frame is required")
  require(columnName != null, "column name is required")
  require(cutoffs.size >= 2, "at least one bin is required")
  require(cutoffs == cutoffs.sorted, "the cutoff points of the bins must be monotonically increasing")
}
