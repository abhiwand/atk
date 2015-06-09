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

package com.intel.intelanalytics.engine.spark.frame.plugins.exporthdfs

import java.nio.file.FileSystem

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.ExportHdfsHiveArgs
import com.intel.intelanalytics.engine.PluginDocAnnotation
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, Invocation }
import com.intel.intelanalytics.engine.spark.HdfsFileStorage
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.frame.plugins.cumulativedist.EcdfJsonFormat
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.engine.spark.{ SparkEngineConfig, HdfsFileStorage }
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
import org.apache.hadoop.fs.Path

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

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

    val frame: SparkFrameData = resolve(arguments.frame)
    // load frame as RDD
    val rdd = frame.data
    FrameExportHdfs.exportToHdfsHive(sc, rdd, arguments.tableName)
    new UnitReturn
  }
}
