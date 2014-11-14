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
import com.intel.intelanalytics.domain.frame.{ CumulativeCount, DataFrame }
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.LegacyFrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Computes a cumulative count
 */
class TallyPlugin extends SparkCommandPlugin[CumulativeCount, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/tally"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Computes a cumulative count.",
    extendedSummary = Some("""
                             |    Compute a cumulative count.
                             |
                             |    A cumulative count is computed by sequentially stepping through the column
                             |    values and keeping track of the the number of times the specified
                             |    *count_value* has been seen up to the current value.
                             |
                             |    Parameters
                             |    ----------
                             |    sample_col : string
                             |        The name of the column from which to compute the cumulative count
                             |
                             |    count_value : string
                             |        The column value to be used for the counts
                             |
                             |    Returns
                             |    -------
                             |    None
                             |
                             |    Examples
                             |    --------
                             |    Consider Frame *my_frame*, which accesses a frame that contains a single
                             |    column *obs*::
                             |
                             |        my_frame.inspect()
                             |
                             |          obs:int32
                             |        /-----------/
                             |            0
                             |            1
                             |            2
                             |            0
                             |            1
                             |            2
                             |
                             |    The cumulative count for column *obs* using *count_value = 1* is obtained
                             |    by::
                             |
                             |        my_frame.tally('obs', '1')
                             |
                             |    The Frame *my_frame* accesses a frame which now contains two columns *obs*
                             |    and *obsCumulativeCount*.
                             |    Column *obs* still has the same data and *obsCumulativeCount* contains the
                             |    cumulative counts::
                             |
                             |        my_frame.inspect()
                             |
                             |          obs:int32        obs_tally:int32
                             |        /----------------------------------/
                             |             0                      0
                             |             1                      1
                             |             2                      1
                             |             0                      1
                             |             1                      2
                             |             2                      2
                             |
                             |    .. versionadded:: 0.8
                             |
                           """.stripMargin)))

  /**
   * Computes a cumulative count
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: CumulativeCount)(implicit invocation: Invocation): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames
    val ctx = sc

    // validate arguments
    val frameId = arguments.frame.id
    val frameMeta = frames.expectFrame(frameId)
    val sampleIndex = frameMeta.schema.columnIndex(arguments.sampleCol)

    // run the operation
    val frameRdd = frames.loadLegacyFrameRdd(ctx, frameMeta)
    val (cumulativeDistRdd, columnName) = (CumulativeDistFunctions.cumulativeCount(frameRdd, sampleIndex, arguments.countVal), "_tally")
    val frameSchema = frameMeta.schema
    val updatedSchema = frameSchema.addColumn(arguments.sampleCol + columnName, DataTypes.float64)

    // save results
    frames.saveLegacyFrame(frameMeta, new LegacyFrameRDD(updatedSchema, cumulativeDistRdd))
  }
}

