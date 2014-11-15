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
import com.intel.intelanalytics.domain.frame.{ DropDuplicates, DataFrame }
import com.intel.intelanalytics.engine.spark.frame.{ LegacyFrameRDD, MiscFrameFunctions }
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
class DropDuplicatesPlugin extends SparkCommandPlugin[DropDuplicates, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/drop_duplicates"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc("Remove duplicate rows.", Some("""
    Remove duplicate rows, keeping only one row per uniqueness criteria match

    Parameters
    ----------
    columns:[str | list of str]
        Column name(s) to identify duplicates.
        If empty, the function will remove duplicates that have the whole row of data identical.

    Examples
    --------
    Remove any rows that have the same data in column * b * as a previously checked row ::

    my_frame.drop_duplicates("b")

    The result is a frame with unique values in column * b *.

    Remove any rows that have the same data in columns * a * and * b * as a previously checked row ::

       my_frame.drop_duplicates([ "a", "b"] )

    The result is a frame with unique values for the combination of columns * a * and * b *.

    Remove any rows that have the whole row identical ::

      my_frame.drop_duplicates()

    The result is a frame where something is different in every row from every other row.
    Each row is unique.

    .versionadded :: 0.8""")))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: DropDuplicates) = 2

  /**
   * Remove duplicate rows, keeping only one row per uniqueness criteria match
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: DropDuplicates)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val frame: DataFrame = frames.expectFrame(arguments.frame.id)
    val rdd = frames.loadLegacyFrameRdd(ctx, arguments.frame.id)
    val columnNames = arguments.unique_columns match {
      case Some(columns) => frame.schema.validateColumnsExist(columns.value).toList
      case None => frame.schema.columnNames
    }
    val duplicatesRemoved: RDD[Array[Any]] = MiscFrameFunctions.removeDuplicatesByColumnNames(rdd, frame.schema, columnNames)
    val rowCount = duplicatesRemoved.count()

    // save results
    frames.saveLegacyFrame(frame, new LegacyFrameRDD(frame.schema, duplicatesRemoved), Some(rowCount))
  }
}

