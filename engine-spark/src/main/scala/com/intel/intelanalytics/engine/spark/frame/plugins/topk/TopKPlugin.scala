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

package com.intel.intelanalytics.engine.spark.frame.plugins.topk

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ DataFrameTemplate, TopK, DataFrame }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.{ Await, ExecutionContext }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate the top (or bottom) K distinct values by count for specified data column.
 */
class TopKPlugin extends SparkCommandPlugin[TopK, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/top_k"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Calculate the top (or bottom) K distinct values by count of a column.",
    extendedSummary = Some("""
    Calculate the top (or bottom) K distinct values by count of a column. The column can be weighted.
    All data elements of weight <= 0 are excluded from the calculation, as are all data elements whose weight is NaN or infinite.
    If there are no data elements of finite weight > 0, then topK is empty.

    Parameters
    ----------
    data_column : str
        The column whose top (or bottom) K distinct values are to be calculated

    k : int
        Number of entries to return (If k is negative, return bottom k)

    weights_column : str (Optional)
        The column that provides weights (frequencies) for the topK calculation.
        Must contain numerical data. Uniform weights of 1 for all items will be used for the calculation if this
        parameter is not provided.

    Returns
    -------

    BigFrame : An object with access to the frame

    Example
    -------
    For this example, we calculate the top 5 movie genres in a data frame.

     >>> top5 = frame.top_k('genre', 5)
     >>> top5.inspect()

      genre:str   count:float64
      ----------------------------
      Drama        738278
      Comedy       671398
      Short        455728
      Documentary  323150
      Talk-Show    265180

   This example calculates the top 3 movies weighted by rating.

     >>> top3 = frame.top_k('genre', 3, weights_column='rating')
     >>> top3.inspect()

     movie:str   count:float64
     -----------------------------
     The Godfather         7689.0
     Shawshank Redemption  6358.0
     The Dark Knight       5426.0

   This example calculates the bottom 3 movie genres in a data frame.

    >>> bottom3 = frame.top_k('genre', -3)
    >>> bottom3.inspect()

       genre:str   count:float64
       ----------------------------
       Musical      26
       War          47
       Film-Noir    595

    ..versionadded :: 0.8 """)))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: TopK) = 3

  /**
   * Calculate the top (or bottom) K distinct values by count for specified data column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: TopK)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val frameId = arguments.frame
    val frame = frames.expectFrame(frameId)
    val columnIndex = frame.schema.columnIndex(arguments.columnName)

    // run the operation
    val frameRdd = frames.loadFrameRdd(ctx, frameId.id)
    val valueDataType = frame.schema.columns(columnIndex)._2
    val (weightsColumnIndexOption, weightsDataTypeOption) = getColumnIndexAndType(frame, arguments.weightsColumn)
    val newFrameName = frames.generateFrameName()
    val newFrame = frames.create(DataFrameTemplate(newFrameName, None))
    val useBottomK = arguments.k < 0
    val topRdd = TopKRDDFunctions.topK(frameRdd, columnIndex, Math.abs(arguments.k), useBottomK,
      weightsColumnIndexOption, weightsDataTypeOption)

    val newSchema = Schema(List(
      (arguments.columnName, valueDataType),
      ("count", DataTypes.float64)
    ))

    val rowCount = topRdd.count()

    // save results
    frames.saveFrame(newFrame, new FrameRDD(newSchema, topRdd), Some(rowCount))
  }

  // TODO: replace getColumnIndexAndType() with methods on Schema

  /**
   * Get column index and data type of a column in a data frame.
   *
   * @param frame Data frame
   * @param columnName Column name
   * @return Option with the column index and data type
   * @deprecated use methods on Schema instead
   */
  private def getColumnIndexAndType(frame: DataFrame, columnName: Option[String]): (Option[Int], Option[DataType]) = {

    val (columnIndexOption, dataTypeOption) = columnName match {
      case Some(columnIndex) => {
        val weightsColumnIndex = frame.schema.columnIndex(columnIndex)
        (Some(weightsColumnIndex), Some(frame.schema.columns(weightsColumnIndex)._2))
      }
      case None => (None, None)
    }
    (columnIndexOption, dataTypeOption)
  }
}
