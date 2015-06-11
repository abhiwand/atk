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

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.covariance

import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import com.intel.intelanalytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData }
import org.apache.spark.frame.FrameRdd
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.domain.schema.FrameSchema
import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.frame.CovarianceMatrixArgs
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.domain.schema.Column
import com.intel.intelanalytics.domain.schema.DataTypes.vector

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate covariance matrix for the specified columns
 * Parameters
 * ----------
 * columns : [ str | list of str ]
 *   The names of the column from which to compute the matrix.
 *   Names should refer to a single column of type vector, or two or more
 *   columns of numeric scalars.
 */
@PluginDoc(oneLine = "Calculate covariance matrix for two or more columns.",
  extended = """Notes
-----
This function applies only to columns containing numerical data.""",
  returns = "A matrix with the covariance values for the columns.")
class CovarianceMatrixPlugin extends SparkCommandPlugin[CovarianceMatrixArgs, FrameEntity] {

  /**
   * The name of the command
   */
  override def name: String = "frame/covariance_matrix"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: CovarianceMatrixArgs)(implicit invocation: Invocation) = 7

  /**
   * Calculate covariance matrix for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance matrix
   * @return value of type declared as the Return type
   */
  override def execute(arguments: CovarianceMatrixArgs)(implicit invocation: Invocation): FrameEntity = {

    val frame: SparkFrameData = resolve(arguments.frame)

    // load frame as RDD
    val frameRdd = frame.data
    val frameSchema = frameRdd.frameSchema
    validateCovarianceArgs(frameSchema, arguments)

    // compute covariance
    val outputColumnDataType = frameSchema.columnDataType(arguments.dataColumnNames(0))
    val outputVectorLength: Option[Long] = outputColumnDataType match {
      case vector(length) => Some(length)
      case _ => None
    }
    val covarianceRdd = CovarianceFunctions.covarianceMatrix(frameRdd, arguments.dataColumnNames, outputVectorLength)

    val outputSchema = getOutputSchema(arguments.dataColumnNames, outputVectorLength)
    tryNew(CreateEntityArgs(description = Some("created by covariance matrix command"))) { newFrame: FrameMeta =>
      if (arguments.matrixName.isDefined) {
        engine.frames.renameFrame(newFrame.meta, FrameName.validate(arguments.matrixName.get))
      }
      save(new SparkFrameData(newFrame.meta, new FrameRdd(outputSchema, covarianceRdd)))
    }.meta
  }

  // Validate input arguments
  private def validateCovarianceArgs(frameSchema: Schema, arguments: CovarianceMatrixArgs): Unit = {
    val dataColumnNames = arguments.dataColumnNames
    if (dataColumnNames.size == 1) {
      frameSchema.requireColumnIsType(dataColumnNames.toList(0), DataTypes.isVectorDataType)
    }
    else {
      require(dataColumnNames.size >= 2, "single vector column, or two or more numeric columns required")
      frameSchema.requireColumnsOfNumericPrimitives(dataColumnNames)
    }
  }

  // Get output schema for covariance matrix
  private def getOutputSchema(dataColumnNames: List[String], outputVectorLength: Option[Long] = None): FrameSchema = {
    val outputColumns = outputVectorLength match {
      case Some(length) => List(Column(dataColumnNames(0), DataTypes.vector(length)))
      case _ => dataColumnNames.map(name => Column(name, DataTypes.float64)).toList
    }
    FrameSchema(outputColumns)
  }
}
