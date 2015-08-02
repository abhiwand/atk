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

package org.trustedanalytics.atk.domain.frame

import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation }

/**
 * Represents a BinColumn object
 */
case class BinColumnArgs(frame: FrameReference,
                         @ArgDoc("""Name of the column to bin.""") columnName: String,
                         @ArgDoc("""Cutoff points of the bins.""") cutoffs: List[Double],
                         @ArgDoc("""If true the lowerbound of the bin will be inclusive
while the upperbound is exclusive if false it is the opposite.""") includeLowest: Option[Boolean],
                         @ArgDoc("""If true values smaller than the first bin or larger
than the last bin will not be given a bin.
If false smaller vales will be in the first bin and larger values will be
in the last.""") strictBinning: Option[Boolean],
                         @ArgDoc("""Name for the created binned column.""") binColumnName: Option[String]) {
  require(frame != null, "frame is required")
  require(columnName != null, "column name is required")
  require(cutoffs.size >= 2, "at least one bin is required")
  require(cutoffs == cutoffs.sorted, "the cutoff points of the bins must be monotonically increasing")
}
