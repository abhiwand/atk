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

package com.intel.intelanalytics.engine.spark.frame.plugins.topk

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameName, DataFrameTemplate, TopKArgs, FrameEntity }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.frame.LegacyFrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.{ Await, ExecutionContext }
import com.intel.intelanalytics.domain.CreateEntityArgs

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate the top (or bottom) K distinct values by count for specified data column.
 */
class TopKPlugin extends SparkCommandPlugin[TopKArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/top_k"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: TopKArgs)(implicit invocation: Invocation) = 3

  /**
   * Calculate the top (or bottom) K distinct values by count for specified data column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: TopKArgs)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames
    val ctx = sc

    // validate arguments
    val frameId = arguments.frame
    val frame = frames.expectFrame(frameId)
    val columnIndex = frame.schema.columnIndex(arguments.columnName)

    // run the operation
    val frameRdd = frames.loadLegacyFrameRdd(ctx, frameId.id)
    val valueDataType = frame.schema.columnTuples(columnIndex)._2
    val (weightsColumnIndexOption, weightsDataTypeOption) = getColumnIndexAndType(frame, arguments.weightsColumn)
    val useBottomK = arguments.k < 0
    val topRdd = TopKRDDFunctions.topK(frameRdd, columnIndex, Math.abs(arguments.k), useBottomK,
      weightsColumnIndexOption, weightsDataTypeOption)

    val newSchema = Schema.fromTuples(List(
      (arguments.columnName, valueDataType),
      ("count", DataTypes.float64)
    ))

    // save results
    frames.tryNewFrame(CreateEntityArgs(description = Some("created by top k command"))) { newFrame =>
      frames.saveLegacyFrame(newFrame.toReference, new LegacyFrameRDD(newSchema, topRdd))
    }
  }

  // TODO: replace getColumnIndexAndType() with methods on Schema

  /**
   * Get column index and data type of a column in a data frame.
   *
   * @param frame Data frame
   * @param columnName Column name
   * @return Option with the column index and data type
   */
  @deprecated("use methods on Schema instead")
  private def getColumnIndexAndType(frame: FrameEntity, columnName: Option[String]): (Option[Int], Option[DataType]) = {

    val (columnIndexOption, dataTypeOption) = columnName match {
      case Some(columnIndex) => {
        val weightsColumnIndex = frame.schema.columnIndex(columnIndex)
        (Some(weightsColumnIndex), Some(frame.schema.columnTuples(weightsColumnIndex)._2))
      }
      case None => (None, None)
    }
    (columnIndexOption, dataTypeOption)
  }
}
