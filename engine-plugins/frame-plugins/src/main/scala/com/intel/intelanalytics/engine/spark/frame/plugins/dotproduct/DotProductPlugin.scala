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

package com.intel.intelanalytics.engine.spark.frame.plugins.dotproduct

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import org.apache.spark.frame.FrameRdd
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin

/** Json conversion for arguments and return value case classes */
object DotProductJsonFormat {
  implicit val dotProductFormat = jsonFormat6(DotProductArgs)
}

import DotProductJsonFormat._

/**
 * Plugin that calculates the dot product for each row in a frame.
 *
 * This is an experimental plugin used by the Netflow POC for scoring. The plugin should be revisited
 * once we support lists as data types.
 */
class DotProductPlugin extends SparkCommandPlugin[DotProductArgs, FrameEntity] {
  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/dot_product"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: DotProductArgs)(implicit invocation: Invocation) = 3

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Calculates the dot product for each row in a frame using values from two equal-length sequences of columns.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running dot-product plugin
   * @return Updated frame with dot product stored in new column
   */
  override def execute(arguments: DotProductArgs)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)
    val frame: SparkFrameData = resolve(arguments.frame)

    // load frame as RDD
    val frameRdd = frame.data
    val frameSchema = frameRdd.frameSchema

    validateDotProductArgs(frameRdd, arguments)

    // run the operation
    val dotProductRdd = DotProductFunctions.dotProduct(frameRdd, arguments.leftColumnNames, arguments.rightColumnNames,
      arguments.defaultLeftValues, arguments.defaultRightValues)

    // save results
    val updatedSchema = frameSchema.addColumn(arguments.dotProductColumnName, DataTypes.float64)
    engine.frames.saveFrameData(frame.meta.toReference, new FrameRdd(updatedSchema, dotProductRdd))
  }

  // Validate input arguments
  private def validateDotProductArgs(frameRdd: FrameRdd, arguments: DotProductArgs): Unit = {
    val frameSchema = frameRdd.frameSchema

    validateColumnTypes(frameRdd, arguments.leftColumnNames)
    validateColumnTypes(frameRdd, arguments.rightColumnNames)

    require(!frameSchema.hasColumn(arguments.dotProductColumnName),
      s"Column name already exists: ${arguments.dotProductColumnName}")
  }

  // Get the size of the column vector
  private def validateColumnTypes(frameRdd: FrameRdd, columnNames: List[String]) = {
    val frameSchema = frameRdd.frameSchema
    require(columnNames.size >= 1, "single vector column, or one or more numeric columns required")

    if (columnNames.size > 1) {
      frameSchema.requireColumnsOfNumericPrimitives(columnNames)
    }
    else {
      val columnDataType = frameSchema.columnDataType(columnNames(0))
      require(columnDataType.isVector || columnDataType.isNumerical,
        s"column ${columnNames(0)} should be of type numeric")
    }
  }

}
