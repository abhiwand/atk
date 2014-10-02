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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.quantiles

import com.intel.intelanalytics.algorithm.{ Quantile, QuantileComposingElement, QuantileTarget }
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ DataFrame, FrameReference, QuantileValues, Quantiles }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.spark.frame.MiscFrameFunctions
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.rdd.RDD

//implicit conversion for PairRDD
import org.apache.spark.SparkContext._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate quantiles on the given column
 */
class QuantilesPlugin extends SparkCommandPlugin[Quantiles, QuantileValues] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "dataframe/quantiles"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Calculate quantiles on given column.",
    extendedSummary = Some(
      """
        |Calculate quantiles on the given column.
        |
        |Parameters
        |----------
        |quantiles : float OR list of float
        |column_name : str
        |    The column to calculate quantile
        |
        |
        |Returns
        |-------
        |dictionary
        |
        |Examples
        |--------
        |
        |Consider BigFrame *my_frame*, which accesses a frame that contains a single column named *final_sale_price*::
        |
        |my_frame.inspect()
        |
        |final_sale_price int32
        ||---------|
        |   100
        |   250
        |   95
        |   179
        |   315
        |   660
        |   540
        |   420
        |   250
        |   335
        |
        |To calculate 10th, 50th, and 100th quantile
        |my_frame.quantiles('final_sale_price', [10, 50, 100])
        |
        |The dictionary will be returned. key is the quantile and value is the quantile value.
        |{10: 95, 50: 315, 100: 660}
        |
        |
        |.. versionchanged:: 0.8
      """.stripMargin)
  ))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: Quantiles) = 7

  /**
   * Calculate quantiles on the given column
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: Quantiles)(implicit user: UserPrincipal, executionContext: ExecutionContext): QuantileValues = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val frameId: FrameReference = arguments.frame
    val frameMeta: DataFrame = frames.expectFrame(frameId.id)
    val frameSchema = frameMeta.schema
    val columnIndex = frameSchema.columnIndex(arguments.columnName)
    val columnDataType = frameSchema.columnDataType(arguments.columnName)

    // run the operation and give the results
    val rdd = frames.loadFrameRdd(ctx, frameMeta)
    val quantileValues = QuantilesFunctions.quantiles(rdd, arguments.quantiles, columnIndex, columnDataType).toList
    QuantileValues(quantileValues)
  }
}

