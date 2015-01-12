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
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.schema.{ FrameSchema, Column, Schema, DataTypes }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.MiscFrameFunctions
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark.frame.LegacyFrameRDD

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
class QuantilesPlugin extends SparkCommandPlugin[QuantilesArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/quantiles"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Column quantiles.",
    extendedSummary = Some("""
                           |    Calculate quantiles on the given column.
                           |
                           |    Parameters
                           |    ----------
                           |    column_name : str
                           |        The column to calculate quantiles
                           |
                           |    quantiles : float OR list of float
                           |
                           |    Returns
                           |    -------
                           |    Frame
                           |      A new Frame with two columns (float64): requested Quantiles and their respective values
                           |
                           |    Examples
                           |    --------
                           |    Consider Frame *my_frame*, which accesses a frame that contains a single
                           |    column *final_sale_price*::
                           |
                           |        my_frame.inspect()
                           |
                           |          final_sale_price:int32
                           |        /------------------------/
                           |                    100
                           |                    250
                           |                     95
                           |                    179
                           |                    315
                           |                    660
                           |                    540
                           |                    420
                           |                    250
                           |                    335
                           |                    
                           |    To calculate 10th, 50th, and 100th quantile::
                           |
                           |        quantiles_frame = my_frame.quantiles('final_sale_price', [10, 50, 100])
                           |  
                           |    A new Frame containing the requested Quantiles and their respective values will be
                           |     returned ::
                           |
                           |       quantiles_frame.inspect()
                           |
                           |        Quantiles:float64   final_sale_price_QuantileValue:float64
                           |       /-----------------------------------------------------------/
                           |               10.0                      95.0
                           |               50.0                     250.0
                           |              100.0                     660.0
                           |
                           |    .. versionchanged:: 0.8
                           |
                            """.stripMargin)))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: QuantilesArgs)(implicit invocation: Invocation) = 7

  /**
   * Calculate quantiles on the given column
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: QuantilesArgs)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames
    val ctx = sc

    // validate arguments
    val frame = frames.expectFrame(arguments.frame.id)
    val frameSchema = frame.schema
    val columnIndex = frameSchema.columnIndex(arguments.columnName)
    val columnDataType = frameSchema.columnDataType(arguments.columnName)
    val quantilesFrameName = FrameName.generate(Some("quantiles"))
    val schema = FrameSchema(List(Column("Quantiles", DataTypes.float64), Column(arguments.columnName + "_QuantileValue", DataTypes.float64)))

    // run the operation and give the results
    val template = DataFrameTemplate(quantilesFrameName, Some("Generated by Quantiles"))
    frames.tryNewFrame(template) { quantilesFrame =>
      val rdd = frames.loadLegacyFrameRdd(ctx, frame)
      val quantileValuesRdd = QuantilesFunctions.quantiles(rdd, arguments.quantiles, columnIndex, columnDataType)
      frames.saveLegacyFrame(quantilesFrame.toReference, new LegacyFrameRDD(schema, quantileValuesRdd))
    }
  }
}

