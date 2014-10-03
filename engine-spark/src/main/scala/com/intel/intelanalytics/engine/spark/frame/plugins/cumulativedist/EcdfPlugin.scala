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
import com.intel.intelanalytics.domain.frame.{ DataFrame, DataFrameTemplate, ECDF }
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Empirical Cumulative Distribution for a column
 */
class EcdfPlugin extends SparkCommandPlugin[ECDF[Long], DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "dataframe/ecdf"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = None

  /**
   * Empirical Cumulative Distribution for a column
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: ECDF[Long])(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val frameId: Long = arguments.frameId
    val frameMeta = frames.expectFrame(frameId)
    val sampleIndex = frameMeta.schema.columnIndex(arguments.sampleCol)

    // run the operation
    val rdd = frames.loadFrameRdd(ctx, frameMeta)
    val newFrame = frames.create(DataFrameTemplate(arguments.name, None))
    val ecdfRdd = CumulativeDistFunctions.ecdf(rdd, sampleIndex, arguments.dataType)

    val rowCount = ecdfRdd.count()

    val columnName = "_ECDF"
    val allColumns = arguments.dataType match {
      case "int32" => List((arguments.sampleCol, DataTypes.int32), (arguments.sampleCol + columnName, DataTypes.float64))
      case "int64" => List((arguments.sampleCol, DataTypes.int64), (arguments.sampleCol + columnName, DataTypes.float64))
      case "float32" => List((arguments.sampleCol, DataTypes.float32), (arguments.sampleCol + columnName, DataTypes.float64))
      case "float64" => List((arguments.sampleCol, DataTypes.float64), (arguments.sampleCol + columnName, DataTypes.float64))
      case _ => List((arguments.sampleCol, DataTypes.string), (arguments.sampleCol + columnName, DataTypes.float64))
    }

    // save results
    frames.saveFrame(newFrame, new FrameRDD(new Schema(allColumns), ecdfRdd), Some(rowCount))
  }
}
