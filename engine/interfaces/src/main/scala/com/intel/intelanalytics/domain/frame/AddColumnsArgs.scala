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

import com.intel.intelanalytics.engine.plugin.{ ArgDoc, Invocation }

case class AddColumnsArgs(
    @ArgDoc("") frame: FrameReference,
    @ArgDoc("") columnNames: List[String],
    @ArgDoc("") columnTypes: List[String],
    @ArgDoc("") udf: Udf,
    @ArgDoc("") columnsAccessed: List[String]) {
  require(frame != null, "frame is required")
  require(columnNames != null, "column names is required")
  for {
    i <- 0 until columnNames.size
  } {
    require(columnNames(i) != "", "column name is required")
  }
  require(columnTypes != null, "column types is required")
  require(columnNames.size == columnTypes.size, "Equal number of column names and types is required")
  require(udf != null, "User defined expression is required")
}
