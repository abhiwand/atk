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

package com.intel.taproot.analytics.engine.frame.plugins.statistics.correlation

import com.intel.taproot.analytics.domain.frame._
import com.intel.taproot.analytics.domain.CreateEntityArgs
import com.intel.taproot.analytics.domain.schema.DataTypes
import com.intel.taproot.analytics.engine.plugin.{ Invocation, PluginDoc }
import org.apache.spark.frame.FrameRdd
import com.intel.taproot.analytics.engine.frame.SparkFrame
import com.intel.taproot.analytics.engine.plugin.SparkCommandPlugin

import com.intel.taproot.analytics.domain.frame.CorrelationMatrixArgs
import com.intel.taproot.analytics.domain.schema.FrameSchema
import com.intel.taproot.analytics.domain.schema.Column

// Implicits needed for JSON conversion
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

/**
 * Calculate correlation matrix for the specified columns
 */
@PluginDoc(oneLine = "Calculate correlation matrix for two or more columns.",
  extended = """Notes
-----
This method applies only to columns containing numerical data.""",
  returns = "A matrix with the correlation values for the columns.")
class CorrelationMatrixPlugin extends SparkCommandPlugin[CorrelationMatrixArgs, FrameEntity] {

  /**
   * The name of the command
   */
  override def name: String = "frame/correlation_matrix"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: CorrelationMatrixArgs)(implicit invocation: Invocation) = 7

  /**
   * Calculate correlation matrix for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for correlation matrix
   * @return value of type declared as the Return type
   */
  override def execute(arguments: CorrelationMatrixArgs)(implicit invocation: Invocation): FrameEntity = {
    val frame: SparkFrame = arguments.frame
    frame.schema.validateColumnsExist(arguments.dataColumnNames)

    val inputDataColumnNamesAndTypes: List[Column] = arguments.dataColumnNames.map({ name => Column(name, DataTypes.float64) })
    val correlationRDD = CorrelationFunctions.correlationMatrix(frame.rdd, arguments.dataColumnNames)

    val schema = FrameSchema(inputDataColumnNamesAndTypes)
    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by correlation matrix command"))) { newFrame: FrameEntity =>
      if (arguments.matrixName.isDefined) {
        engine.frames.renameFrame(newFrame, FrameName.validate(arguments.matrixName.get))
      }
      newFrame.save(new FrameRdd(schema, correlationRDD))
    }
  }
}
