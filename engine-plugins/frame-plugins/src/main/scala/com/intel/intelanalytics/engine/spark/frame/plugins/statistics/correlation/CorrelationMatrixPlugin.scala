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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.correlation

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.{ CreateEntityArgs, Naming }
import com.intel.intelanalytics.domain.schema.Column
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.FrameSchema
import com.intel.intelanalytics.domain.schema.{ Column, FrameSchema, DataTypes, Schema }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.plugin.Invocation
import org.apache.spark.frame.FrameRdd
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.rdd.RDD

import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.domain.frame.CorrelationMatrixArgs
import com.intel.intelanalytics.domain.schema.FrameSchema
import com.intel.intelanalytics.domain.command.CommandDoc
import scala.Some
import com.intel.intelanalytics.domain.schema.Column
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate correlation matrix for the specified columns
 * Parameters
 * ----------
 * columns : [ str | list of str ]
 *   The names of the column from which to compute the matrix.
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

    val frame: SparkFrameData = resolve(arguments.frame)
    frame.meta.schema.validateColumnsExist(arguments.dataColumnNames)
    // load frame as RDD
    val rdd = frame.data

    val inputDataColumnNamesAndTypes: List[Column] = arguments.dataColumnNames.map({ name => Column(name, DataTypes.float64) })
    val correlationRDD = Correlation.correlationMatrix(rdd, arguments.dataColumnNames)

    val schema = FrameSchema(inputDataColumnNamesAndTypes)
    tryNew(CreateEntityArgs(description = Some("created by correlation matrix command"))) { newFrame: FrameMeta =>
      if (arguments.matrixName.isDefined) {
        engine.frames.renameFrame(newFrame.meta, FrameName.validate(arguments.matrixName.get))
      }
      save(new SparkFrameData(newFrame.meta, new FrameRdd(schema, correlationRDD)))
    }.meta
  }
}
