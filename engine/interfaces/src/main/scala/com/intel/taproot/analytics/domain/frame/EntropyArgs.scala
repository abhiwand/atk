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

/**
 * Command for calculating the Shannon entropy of a column in a data frame.
 *
 * @param dataColumn Name of the column to compute entropy.
 * @param weightsColumn Optional. Name of the column that provides weights (frequencies).
 *
 */
case class EntropyArgs(frame: FrameReference, dataColumn: String, weightsColumn: Option[String] = None) {
  require(frame != null, "frame is required")
  require(dataColumn != null, "column name is required")
}