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
 * Command for retrieving the top (or bottom) K distinct values by count for a specified column.
 *
 * @param frame Reference to the input data frame
 * @param columnName Column name
 * @param k Number of entries to return (if negative, return the bottom K values, else return top K)
 * @param weightsColumn Optional. Name of the column that provides weights (frequencies).
 */
case class TopKArgs(frame: FrameReference, columnName: String, k: Int,
                    weightsColumn: Option[String] = None) {
  require(frame != null, "frame is required")
  require(columnName != null, "column name is required")
  require(k != 0, "k should not be equal to zero")
}
