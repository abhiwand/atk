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
 * Command for calculating percentiles values
 * @param frame id of the data frame
 * @param columnName name of the column to find percentiles
 * @param quantiles the percentiles to calculate value for
 */
case class QuantilesArgs(frame: FrameReference, columnName: String, quantiles: List[Double])
