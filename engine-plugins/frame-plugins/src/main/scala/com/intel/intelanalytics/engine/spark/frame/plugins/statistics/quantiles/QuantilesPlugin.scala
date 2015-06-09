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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.quantiles

import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.schema.{ FrameSchema, Column, DataTypes }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.domain.CreateEntityArgs
import org.apache.spark.frame.FrameRdd

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
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: QuantilesArgs)(implicit invocation: Invocation) = 4

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

    // validate arguments
    val frame = frames.expectFrame(arguments.frame)
    val frameSchema = frame.schema
    val columnIndex = frameSchema.columnIndex(arguments.columnName)
    val columnDataType = frameSchema.columnDataType(arguments.columnName)
    val schema = FrameSchema(List(Column("Quantiles", DataTypes.float64), Column(arguments.columnName + "_QuantileValue", DataTypes.float64)))

    // run the operation and give the results
    val template = DataFrameTemplate(None, Some("Generated by Quantiles"))
    frames.tryNewFrame(CreateEntityArgs(description = Some("created by quantiles commands"))) { quantilesFrame =>
      val rdd = frames.loadFrameData(sc, frame)
      //frames should have their row count set unless it is an empty frame
      val quantileValuesRdd = QuantilesFunctions.quantiles(rdd, arguments.quantiles, columnIndex, frame.rowCount.getOrElse(0))
      frames.saveFrameData(quantilesFrame.toReference, new FrameRdd(schema, quantileValuesRdd))
    }
  }
}
