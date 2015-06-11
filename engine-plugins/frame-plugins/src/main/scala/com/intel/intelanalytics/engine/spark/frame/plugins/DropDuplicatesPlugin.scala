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

import com.intel.intelanalytics.domain.frame.{ DropDuplicatesArgs, FrameEntity }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
import com.intel.intelanalytics.engine.spark.frame.{ LegacyFrameRdd, MiscFrameFunctions }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import org.apache.spark.rdd.RDD

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Remove duplicate rows, keeping only one row per uniqueness criteria match
 *
 * Parameters
 * ----------
 * columns : [str | list of str] (optional)
 *   Column name(s) to identify duplicates.
 *   Default is the entire row is compared.
 */
@PluginDoc(oneLine = "Modify the current frame, removing duplicate rows.",
  extended = """Remove data rows which are the same as other rows.
The entire row can be checked for duplication, or the search for duplicates
can be limited to one or more columns.
This modifies the current frame.""")
class DropDuplicatesPlugin extends SparkCommandPlugin[DropDuplicatesArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/drop_duplicates"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: DropDuplicatesArgs)(implicit invocation: Invocation) = 2

  /**
   * Remove duplicate rows, keeping only one row per uniqueness criteria match
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DropDuplicatesArgs)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    // validate arguments
    val frame: FrameEntity = frames.expectFrame(arguments.frame)
    val rdd = frames.loadLegacyFrameRdd(sc, arguments.frame)
    val columnNames = arguments.unique_columns match {
      case Some(columns) => frame.schema.validateColumnsExist(columns.value).toList
      case None => frame.schema.columnNames
    }
    val duplicatesRemoved: RDD[Array[Any]] = MiscFrameFunctions.removeDuplicatesByColumnNames(rdd, frame.schema, columnNames)

    // save results
    frames.saveLegacyFrame(frame.toReference, new LegacyFrameRdd(frame.schema, duplicatesRemoved))
  }
}
