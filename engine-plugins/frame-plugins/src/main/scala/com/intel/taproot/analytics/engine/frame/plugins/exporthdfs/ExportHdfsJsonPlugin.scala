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

package com.intel.taproot.analytics.engine.frame.plugins.exporthdfs

import java.nio.file.FileSystem

import com.intel.taproot.analytics.UnitReturn
import com.intel.taproot.analytics.domain.command.CommandDoc
import com.intel.taproot.analytics.domain.frame.ExportHdfsJsonArgs
import com.intel.taproot.analytics.engine.plugin.{ Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.{ EngineConfig, HdfsFileStorage }
import com.intel.taproot.analytics.engine.frame.SparkFrame
import com.intel.taproot.analytics.engine.plugin.SparkCommandPlugin
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

// Implicits needed for JSON conversion
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

/**
 * Export a frame to json file
 */
@PluginDoc(oneLine = "Write current frame to HDFS in JSON format.",
  extended = "Export the frame to a file in JSON format as a Hadoop file.")
class ExportHdfsJsonPlugin extends SparkCommandPlugin[ExportHdfsJsonArgs, UnitReturn] {

  /**
   * The name of the command
   */
  override def name: String = "frame/export_to_json"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ExportHdfsJsonArgs)(implicit invocation: Invocation) = 5

  /**
   * Calculate covariance for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance
   * @return value of type declared as the Return type
   */
  override def execute(arguments: ExportHdfsJsonArgs)(implicit invocation: Invocation): UnitReturn = {
    val fileStorage = new HdfsFileStorage(EngineConfig.fsRoot)
    require(!fileStorage.exists(new Path(arguments.folderName)), "File or Directory already exists")
    val frame: SparkFrame = arguments.frame
    FrameExportHdfs.exportToHdfsJson(frame.rdd, arguments.folderName, arguments.count, arguments.offset)
  }

}
