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
import com.intel.intelanalytics.domain.frame.{ FrameAddColumns, DataFrame }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.spark.frame.PythonRDDStorage
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Adds one or more new columns to the frame by evaluating the given func on each row.
 */
class AddColumnsPlugin extends SparkCommandPlugin[FrameAddColumns, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/add_columns"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = None

  /**
   * Adds one or more new columns to the frame by evaluating the given func on each row.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: FrameAddColumns)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val pythonRDDStorage = new PythonRDDStorage(frames)
    val ctx = invocation.sparkContext

    // validate arguments
    val frameId = arguments.frame.id
    val columnNames = arguments.columnNames
    val columnTypes = arguments.columnTypes
    val expression = arguments.expression // Python Wrapper containing lambda expression
    val frameMeta = frames.expectFrame(arguments.frame)
    val schema = frameMeta.schema
    val oldColumns = schema.columns

    // run the operation and save results
    var newColumns = schema.columns
    for {
      i <- 0 until columnNames.size
    } {
      val columnName = columnNames(i)
      val columnType = columnTypes(i)

      if (schema.columns.indexWhere(columnTuple => columnTuple._1 == columnName) >= 0)
        throw new IllegalArgumentException(s"Duplicate column name: $columnName")

      // Update the schema
      newColumns = newColumns :+ (columnName, DataTypes.toDataType(columnType))
    }

    // Update the data
    val pyRdd = pythonRDDStorage.createPythonRDD(frameId, expression, invocation.sparkContext)
    val converter = DataTypes.parseMany(newColumns.map(_._2).toArray)(_)
    var newFrame = frames.updateSchema(frameMeta, newColumns)
    try {
      pythonRDDStorage.persistPythonRDD(newFrame, pyRdd, converter, skipRowCount = true)
    }
    catch {
      case ex: Exception => {
        //reverting back to old schema
        newFrame = frames.updateSchema(frameMeta, oldColumns)
        throw ex
      }
    }
    newFrame
  }
}
