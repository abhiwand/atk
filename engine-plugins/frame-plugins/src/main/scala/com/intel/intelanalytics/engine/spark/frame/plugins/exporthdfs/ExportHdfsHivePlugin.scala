//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

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

