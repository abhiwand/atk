//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.engine.spark.frame.PythonRDDStorage

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Copies specified columns into a new BigFrame object, optionally renaming them and/or filtering them
 */
class CopyFramePlugin extends SparkCommandPlugin[CopyFrameArgs, FrameEntity] {

  override def name: String = "frame/copy"

  override def numberOfJobs(arguments: CopyFrameArgs)(implicit invocation: Invocation) = {
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
  override def execute(arguments: CopyFrameArgs)(implicit invocation: Invocation): FrameEntity = {

    val sourceFrame: SparkFrameData = resolve(arguments.frame)
    val sourceRdd = sourceFrame.data

    val finalRdd = if (arguments.where.isDefined) {
      val finalSchema = arguments.columns.isDefined match {
        case true => sourceRdd.frameSchema.copySubsetWithRename(arguments.columns.get)
        case false => sourceRdd.frameSchema
      }

      // predicated copy - the column select is baked into the 'where' function, see Python client spark.py
      // Note: Update if UDF wrapping logic ever moves out of the client and into the server
      PythonRDDStorage.mapWith(sourceRdd, arguments.where.get, finalSchema, sc)
    }
    else {
      if (arguments.columns.isDefined) {
        sourceRdd.selectColumnsWithRename(arguments.columns.get)
      }
      else {
        sourceRdd
      }
    }

    val frames = engine.frames
    frames.tryNewFrame(CreateEntityArgs(description = Some("created by copy command"))) {
      newFrame => frames.saveFrameData(newFrame.toReference, finalRdd)
    }
  }
}
