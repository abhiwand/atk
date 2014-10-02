//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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
import com.intel.intelanalytics.domain.frame.{ FrameProject, DataFrame }
import com.intel.intelanalytics.domain.schema.Schema
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Copies specified columns into a new BigFrame object, optionally renaming them.
 */
class ProjectPlugin extends SparkCommandPlugin[FrameProject, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "dataframe/project"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = None

  /**
   * Copies specified columns into a new BigFrame object, optionally renaming them.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: FrameProject)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val sourceFrameID = arguments.frame.id
    val sourceFrame = frames.expectFrame(sourceFrameID)
    val projectedFrameID = arguments.projectedFrame.id
    val projectedFrame = frames.expectFrame(projectedFrameID)
    val columns = arguments.columns
    val schema = sourceFrame.schema

    val columnIndices = for {
      col <- columns
      columnIndex = schema.columns.indexWhere(columnTuple => columnTuple._1 == col)
    } yield columnIndex

    if (columnIndices.contains(-1)) {
      throw new IllegalArgumentException(s"Invalid list of columns: ${arguments.columns.toString()}")
    }

    // run the operation
    val resultRdd = frames.loadFrameData(ctx, sourceFrameID)
      .map(row => {
        for { i <- columnIndices } yield row(i)
      }.toArray)

    val projectedColumns = arguments.newColumnNames match {
      case empty if empty.size == 0 => for { i <- columnIndices } yield schema.columns(i)
      case _ =>
        for { i <- 0 until columnIndices.size }
          yield (arguments.newColumnNames(i), schema.columns(columnIndices(i))._2)
    }

    // save results
    frames.saveFrameData(projectedFrame, new FrameRDD(new Schema(projectedColumns.toList), resultRdd), sourceFrame.rowCount)
  }
}
