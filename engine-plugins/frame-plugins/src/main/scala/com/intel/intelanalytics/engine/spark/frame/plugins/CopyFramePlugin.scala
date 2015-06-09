/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.engine.spark.frame.PythonRddStorage

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Copies specified columns into a new Frame object, optionally renaming them and/or filtering them
 */
@PluginDoc(oneLine = "",
  extended = "",
  returns = "")
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
      PythonRddStorage.mapWith(sourceRdd, arguments.where.get, finalSchema, sc)
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
    frames.tryNewFrame(CreateEntityArgs(name = arguments.name, description = Some("created by copy command"))) {
      newFrame => frames.saveFrameData(newFrame.toReference, finalRdd)
    }
  }
}
