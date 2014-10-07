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

package com.intel.intelanalytics.engine.spark.frame.plugins.cumulativedist

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ CumulativePercentSum, DataFrame }
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Compute a cumulative percent sum.
 */
class CumulativePercentPlugin extends SparkCommandPlugin[CumulativePercentSum, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/cumulative_percent"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Computes a cumulative percent sum.",
    extendedSummary = Some("""
    Compute a cumulative percent sum.

    A cumulative percent sum is computed by sequentially stepping through the column values and keeping track of the
    current percentage of the total sum accounted for at the current value.

    Parameters
    ----------
    sample_col: string
      The name of the column from which to compute the cumulative percent sum

    Returns
    -------
    BigFrame
      A new object accessing a new frame containing the original columns appended with a column containing the cumulative percent sums

    Notes
    -----
      This function applies only to columns containing numerical data.  Although this function will execute for columns
      containing negative values, the interpretation of the result will change (e.g., negative percentages).

    Examples
    --------
    Consider BigFrame * my_frame * accessing a frame that contains a single column named * obs *::

        my_frame.inspect()

        obs int32
                             |---------|
          0
          1
          2
          0
          1
          2

    The cumulative percent sum for column * obs * is obtained by ::

    cps_frame = my_frame.cumulative_percent('obs')

    The new frame accessed by BigFrame * cps_frame * contains two columns * obs * and * obsCumulativePercentSum *.
    They contain the original data and the cumulative percent sum, respectively ::

        cps_frame.inspect()

        obs   int32   obs_cumulative_percent float64
                             |-------------------------------------------|
          0                   0.0
          1                   0.16666666
          2                   0.5
          0                   0.5
          1                   0.66666666
          2                   1.0

      ..versionadded :: 0.8 """)))

  /**
   * Compute a cumulative percent sum.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: CumulativePercentSum)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val frameId = arguments.frame.id
    val frameMeta = frames.expectFrame(frameId)
    val sampleIndex = frameMeta.schema.columnIndex(arguments.sampleCol)

    // run the operation
    val frameRdd = frames.loadFrameRdd(ctx, frameId)
    val (cumulativeDistRdd, columnName) = (CumulativeDistFunctions.cumulativePercentSum(frameRdd, sampleIndex), "_cumulative_percent")
    val frameSchema = frameMeta.schema
    val allColumns = frameSchema.columns :+ (arguments.sampleCol + columnName, DataTypes.float64)

    // save results
    frames.saveFrame(frameMeta, new FrameRDD(new Schema(allColumns), cumulativeDistRdd))
  }
}
