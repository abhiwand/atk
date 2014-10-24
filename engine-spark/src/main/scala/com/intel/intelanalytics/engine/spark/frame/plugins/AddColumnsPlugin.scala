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
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.plugin.Transformation
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData, PythonRDDStorage }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion

import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Adds one or more new columns to the frame by evaluating the given func on each row.
 */
class AddColumnsPlugin extends SparkCommandPlugin[FrameAddColumns, FrameReference]
  with Transformation[FrameAddColumns, FrameReference] {

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
  override def execute(invocation: SparkInvocation,
                       arguments: FrameAddColumns,
                       returnValue: FrameReference)(implicit user: UserPrincipal,
                                                            executionContext: ExecutionContext): FrameReference = {
    val frame = invocation.resolve[SparkFrameData](arguments.frame)
    val oldSchema = frame.meta.schema
    val newColumns = arguments.columnNames.zip(
                              arguments.columnTypes.map(DataTypes.toDataType)).toList
    val newSchema = oldSchema.addColumns(newColumns)

    // Update the data
    val pyRdd = PythonRDDStorage.createPythonRDD(frame.data, arguments.expression)
    val converter = DataTypes.parseMany(newColumns.map(_._2).toArray)(_)
    val rdd = PythonRDDStorage.pyRDDToFrameRDD(newSchema, pyRdd, converter)
    val ret = invocation.resolve[FrameMeta](returnValue)
    new SparkFrameData(ret.meta.copy(schema = newSchema), rdd)
  }
}
