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

import com.intel.intelanalytics.engine.ArgDocAnnotation
import com.intel.intelanalytics.engine.plugin.ArgDoc

/**
 * Input arguments class for export to CSV
 */
case class ExportHdfsCsvArgs(frame: FrameReference,
                             folderName: String,
                             separator: Option[Char] = None,
                             count: Option[Int] = None,
                             offset: Option[Int] = None) {
  require(frame != null, "frame is required")
  require(folderName != null, "folder name is required")
}

/**
 * Input arguments class for export to JSON
 */
case class ExportHdfsJsonArgs(frame: FrameReference,
                              folderName: String,
                              count: Option[Int] = None,
                              offset: Option[Int] = None) {
  require(frame != null, "frame is required")
  require(folderName != null, "folder name is required")
}

/**
 * Input arguments class for export to Hive
 */
case class ExportHdfsHiveArgs(frame: FrameReference,
                              @ArgDoc("The name of the Hive table that will contain the exported frame") tableName: String) {
  require(frame != null, "frame is required")
  require(tableName != null, "table name is required")
}
