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

package com.intel.intelanalytics.engine.spark.frame.plugins.dotproduct

import com.intel.intelanalytics.domain.frame.FrameReference

/**
 * Arguments for dot-product plugin
 *
 * The dot-product plugin uses the left column values, and the right column values to compute the dot product for each row.
 * The column data may contain numbers, or lists of numbers.
 *
 * @param frame Data frame
 * @param leftColumnNames Left column names
 * @param rightColumnNames Right column names
 * @param dotProductColumnName Name of column for storing results of dot-product
 * @param defaultLeftValues Optional default values used to substitute null values in the left columns
 * @param defaultRightValues Optional default values used to substitute null values in the right columns
 */
case class DotProductArgs(frame: FrameReference,
                          leftColumnNames: List[String],
                          rightColumnNames: List[String],
                          dotProductColumnName: String,
                          defaultLeftValues: Option[List[Double]] = None,
                          defaultRightValues: Option[List[Double]] = None) {
  require(frame != null, "frame is required")
  require(leftColumnNames.size > 0, "number of left columns cannot be zero")
  require(dotProductColumnName != null, "dot product column name cannot be null")
}
