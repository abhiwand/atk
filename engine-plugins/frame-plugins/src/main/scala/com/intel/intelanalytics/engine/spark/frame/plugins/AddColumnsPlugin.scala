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

import com.intel.intelanalytics.domain.UriReference
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.schema.{ FrameSchema, Column }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ PythonRddStorage, SparkFrameData }
import org.apache.spark.frame.FrameRdd
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.sql
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Adds one or more new columns to the frame by evaluating the given func on each row.
 */
@PluginDoc(oneLine = "",
  extended = "",
  returns = "")
class AddColumnsPlugin extends SparkCommandPlugin[AddColumnsArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/add_columns"

  /**
   * Adds one or more new columns to the frame by evaluating the given func on each row.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: AddColumnsArgs)(implicit invocation: Invocation): FrameEntity = {
    val frame: SparkFrameData = resolve(arguments.frame)

    val newColumns = arguments.columnNames.zip(arguments.columnTypes.map(x => x: DataType))
    val columnList = newColumns.map { case (name, dataType) => Column(name, dataType) }
    val newSchema = new FrameSchema(columnList)
    val finalSchema = frame.meta.schema.addColumns(columnList)

    // Update the data
    // What we pass to PythonRddStorage is a stripped down version of FrameRdd if columnsAccessed is defined
    val rdd = arguments.columnsAccessed.isEmpty match {
      case true => PythonRddStorage.mapWith(frame.data, arguments.udf, newSchema, sc)
      case false => PythonRddStorage.mapWith(frame.data.selectColumns(arguments.columnsAccessed), arguments.udf, newSchema, sc)
    }

    save(new SparkFrameData(frame.meta.withSchema(finalSchema), frame.data.zipFrameRdd(rdd))).meta
  }
}
