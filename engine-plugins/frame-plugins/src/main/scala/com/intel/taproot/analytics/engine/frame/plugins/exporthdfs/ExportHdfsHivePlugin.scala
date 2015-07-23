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
import com.intel.taproot.analytics.domain.DomainJsonProtocol
import com.intel.taproot.analytics.domain.command.CommandDoc
import com.intel.taproot.analytics.domain.frame.ExportHdfsHiveArgs
import com.intel.taproot.analytics.engine.PluginDocAnnotation
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.HdfsFileStorage
import com.intel.taproot.analytics.engine.EngineConfig
import com.intel.taproot.analytics.engine.frame.SparkFrame
import com.intel.taproot.analytics.engine.frame.plugins.cumulativedist.EcdfJsonFormat
import com.intel.taproot.analytics.engine.plugin.SparkCommandPlugin
import com.intel.taproot.analytics.engine.{ EngineConfig, HdfsFileStorage }
import com.intel.taproot.analytics.engine.plugin.SparkCommandPlugin
import org.apache.hadoop.fs.Path

// Implicits needed for JSON conversion
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

/**
 * Export a frame to csv file
 */
@PluginDoc(oneLine = "Write current frame to Hive table. Table must not exist in Hive",
  extended = """Export of Vectors is not currently supported""",
  returns = "None")
class ExportHdfsHivePlugin extends SparkCommandPlugin[ExportHdfsHiveArgs, UnitReturn] {

  /**
   * The name of the command
   */
  override def name: String = "frame/export_to_hive"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ExportHdfsHiveArgs)(implicit invocation: Invocation) = 5

  /**
   * Calculate covariance for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance
   * @return value of type declared as the Return type
   */
  override def execute(arguments: ExportHdfsHiveArgs)(implicit invocation: Invocation): UnitReturn = {
    val frame: SparkFrame = arguments.frame
    FrameExportHdfs.exportToHdfsHive(sc, frame.rdd, arguments.tableName)
  }
}
