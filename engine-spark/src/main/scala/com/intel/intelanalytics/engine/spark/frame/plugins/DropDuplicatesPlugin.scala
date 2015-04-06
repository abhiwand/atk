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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ DropDuplicatesArgs, FrameEntity }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ LegacyFrameRdd, MiscFrameFunctions }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.rdd.RDD

import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.domain.schema.Schema

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Remove duplicate rows, keeping only one row per uniqueness criteria match
 */
class DropDuplicatesPlugin extends SparkCommandPlugin[DropDuplicatesArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/drop_duplicates"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: DropDuplicatesArgs)(implicit invocation: Invocation) = 2

  /**
   * Remove duplicate rows, keeping only one row per uniqueness criteria match
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DropDuplicatesArgs)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames
    val ctx = sc

    // validate arguments
    val frame: FrameEntity = frames.expectFrame(arguments.frame)
    val rdd = frames.loadLegacyFrameRdd(ctx, arguments.frame)
    val columnNames = arguments.unique_columns match {
      case Some(columns) => frame.schema.validateColumnsExist(columns.value).toList
      case None => frame.schema.columnNames
    }
    val duplicatesRemoved: RDD[Array[Any]] = MiscFrameFunctions.removeDuplicatesByColumnNames(rdd, frame.schema, columnNames)

    // save results
    frames.saveLegacyFrame(frame.toReference, new LegacyFrameRdd(frame.schema, duplicatesRemoved))
  }
}
