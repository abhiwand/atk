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
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData, PythonRDDStorage, FrameRDD }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.spark.frame.{ PythonRDDStorage, FrameRDD }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

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
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: FrameCopy)(implicit invocation: Invocation): DataFrame = {

    val sourceFrame: SparkFrameData = resolve(arguments.frame)
    val (newSchema, indices) = arguments.columns match {
      case None => (sourceFrame.meta.schema, null) // full copy
      case Some(cols) => sourceFrame.meta.schema.getRenamedSchemaAndIndices(cols) // partial copy
    }

    val newFrame: FrameMeta = create[FrameMeta](arguments.name)

    val data: FrameRDD = arguments.where match {
      case None =>
        arguments.columns match {
          case None => sourceFrame.data // full copy
          case Some(x) => FrameRDD.toFrameRDD(newSchema, // partial copy
            sourceFrame.data
              .map(row => indices.map(row(_)).toArray))
        }
      case Some(where) =>
        // predicated copy - the column select is baked into the 'where' function, see Python client spark.py
        // TODO - update if UDF wrapping logic ever moves out of the client and into the server
        PythonRDDStorage.pyMappish(sourceFrame.data, where, sourceFrame.meta.schema)
    }

    // save results
    save(new SparkFrameData(newFrame.meta.withSchema(newSchema), data)).meta
  }
}
