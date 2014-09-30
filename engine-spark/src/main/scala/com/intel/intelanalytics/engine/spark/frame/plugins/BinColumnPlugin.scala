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
import com.intel.intelanalytics.domain.frame.{ DataFrameTemplate, BinColumn, DataFrame }
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.{ DiscretizationFunctions, SparkEngineConfig }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.{ Await, ExecutionContext }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Column values into bins.
 *
 * Two types of binning are provided: equalwidth and equaldepth.
 *
 * Equal width binning places column values into bins such that the values in each bin fall within the same
 * interval and the interval width for each bin is equal.
 *
 * Equal depth binning attempts to place column values into bins such that each bin contains the same number
 * of elements
 */
class BinColumnPlugin extends SparkCommandPlugin[BinColumn, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "dataframe/bin_column"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = None

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: BinColumn) = 7

  /**
   *
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: BinColumn)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val frameId: Long = arguments.frame.id
    val frameMeta = frames.expectFrame(frameId)
    val columnIndex = frameMeta.schema.columnIndex(arguments.columnName)
    if (frameMeta.schema.columns.indexWhere(columnTuple => columnTuple._1 == arguments.binColumnName) >= 0)
      throw new IllegalArgumentException(s"Duplicate column name: ${arguments.binColumnName}")

    // run the operation and save results
    val rdd = frames.loadFrameRdd(ctx, frameId)
    val newFrame = frames.create(DataFrameTemplate(arguments.name, None))
    val allColumns = frameMeta.schema.columns :+ (arguments.binColumnName, DataTypes.int32)
    arguments.binType match {
      case "equalwidth" =>
        val binnedRdd = DiscretizationFunctions.binEqualWidth(columnIndex, arguments.numBins, rdd)
        val rowCount = binnedRdd.count()
        frames.saveFrame(newFrame, new FrameRDD(new Schema(allColumns), binnedRdd), Some(rowCount))
      case "equaldepth" =>
        val binnedRdd = DiscretizationFunctions.binEqualDepth(columnIndex, arguments.numBins, rdd)
        val rowCount = binnedRdd.count()
        frames.saveFrame(newFrame, new FrameRDD(new Schema(allColumns), binnedRdd), Some(rowCount))
      case _ => throw new IllegalArgumentException(s"Invalid binning type: ${arguments.binType.toString}")
    }
    frames.updateSchema(newFrame, allColumns)
  }
}
