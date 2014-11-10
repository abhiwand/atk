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
import com.intel.intelanalytics.domain.frame.{ FrameName, DataFrameTemplate, FrameCopy, DataFrame }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.spark.frame.{ PythonRDDStorage, FrameRDD }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Copies specified columns into a new BigFrame object, optionally renaming them and/or filtering them
 */
class CopyPlugin extends SparkCommandPlugin[FrameCopy, DataFrame] {

  override def name: String = "frame/copy"

  override def doc: Option[CommandDoc] = None // Provided in the Python client, since there is special logic there

  override def numberOfJobs(arguments: FrameCopy) = {
    arguments.where match {
      case Some(function) => 2 // predicated copy requires a row count operation
      case None => 1
    }
  }

  /**
   * Create a copy of frame with options: select only certain columns, rename columns, condition which rows are copied
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: FrameCopy)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    val sourceFrame = frames.expectFrame(arguments.frame)
    val (newSchema, indices) = arguments.columns match {
      case None => (sourceFrame.schema, null) // full copy
      case Some(cols) => sourceFrame.schema.getRenamedSchemaAndIndices(cols) // partial copy
    }

    // run the operation
    val template = DataFrameTemplate(FrameName.validateOrGenerate(arguments.name), Some("copy"))
    frames.tryNewFrame(template) { newFrame =>
      val copiedFrame = frames.updateSchema(newFrame, newSchema)
      if (arguments.where.isEmpty) {
        val rdd: FrameRDD = arguments.columns match {
          case None => frames.loadFrameRDD(ctx, sourceFrame) // full copy
          case Some(x) => FrameRDD.toFrameRDD(newSchema, // partial copy
            frames.loadFrameRDD(ctx, sourceFrame)
              .map(row => {
                for { i <- indices } yield row(i)
              }.toArray))
        }
        frames.saveFrame(copiedFrame, rdd, Some(sourceFrame.rowCount))
      }
      else {
        // predicated copy - the column select is baked into the 'where' function, see Python client spark.py
        // TODO - update if UDF wrapping logic ever moves out of the client and into the server
        val pythonRDDStorage = new PythonRDDStorage(frames)
        val pyRdd = pythonRDDStorage.createPythonRDD(sourceFrame.id, arguments.where.get, ctx)
        val converter = DataTypes.parseMany(newSchema.columns.map(_.dataType).toArray)(_)
        val rowCount = pythonRDDStorage.persistPythonRDD(copiedFrame, pyRdd, converter, skipRowCount = false)
        frames.updateRowCount(copiedFrame, rowCount)
      }
    }
  }
}
