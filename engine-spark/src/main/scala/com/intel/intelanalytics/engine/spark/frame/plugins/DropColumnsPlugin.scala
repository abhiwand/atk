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
import com.intel.intelanalytics.domain.frame.{ FrameDropColumns, DataFrame }
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Remove columns from a frame
 */
class DropColumnsPlugin extends SparkCommandPlugin[FrameDropColumns, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/drop_columns"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Remove columns from the frame.",
    extendedSummary = Some("""
    Remove columns from the frame.  They are deleted.

    Parameters
    ----------
    columns: str OR list of str
        column name OR list of column names to be removed from the frame

    Notes
    -----
    Deleting the last column in a frame leaves the frame empty.

    Examples
    --------
    For this example, BigFrame object * my_frame * accesses a frame with columns * column_a *, * column_b *, * column_c * and * column_d *.
    Eliminate columns * column_b * and * column_d *::
    my_frame.drop_columns([column_b, column_d])
    Now the frame only has the columns * column_a * and * column_c *.
    For further examples, see: ref: `example_frame.drop_columns`""")))

  /**
   * Remove columns from a frame.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: FrameDropColumns)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val frameId = arguments.frame.id
    val columns = arguments.columns
    val frameMeta = frames.expectFrame(arguments.frame)
    val schema = frameMeta.schema

    // run the operation
    val columnIndices = {
      for {
        col <- columns
        columnIndex = schema.columns.indexWhere(columnTuple => columnTuple._1 == col)
      } yield columnIndex
    }.sorted.distinct

    val resultRDD = columnIndices match {
      case invalidColumns if invalidColumns.contains(-1) =>
        throw new IllegalArgumentException(s"Invalid list of columns: [${arguments.columns.mkString(", ")}]")
      case allColumns if allColumns.length == schema.columns.length =>
        frames.loadFrameRdd(ctx, frameId).filter(_ => false)
      case singleColumn if singleColumn.length == 1 =>
        frames.loadFrameRdd(ctx, frameMeta)
          .map(row => row.take(singleColumn(0)) ++ row.drop(singleColumn(0) + 1))
      case multiColumn =>
        frames.loadFrameRdd(ctx, frameId)
          .map(row => row.zipWithIndex.filter(elem => multiColumn.contains(elem._2) == false).map(_._1))
    }

    val dataFrame = frames.dropColumns(frameMeta, columnIndices)

    // save results
    frames.saveFrame(dataFrame, new FrameRDD(dataFrame.schema, resultRDD))
  }
}
