///*
//// Copyright (c) 2015 Intel Corporation 
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////      http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//*/
//
//package com.intel.taproot.analytics.engine.spark.frame.plugins.dimensionalityreduction
//
//import breeze.numerics._
//import com.intel.taproot.analytics.domain.frame._
//import com.intel.taproot.analytics.domain.schema.{ DataTypes, Schema }
//import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
//import com.intel.taproot.analytics.engine.spark.frame.{ SparkFrameData }
//import org.apache.spark.frame.FrameRdd
//import com.intel.taproot.analytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
//import com.intel.taproot.analytics.domain.schema.FrameSchema
//import com.intel.taproot.analytics.domain.CreateEntityArgs
//import com.intel.taproot.analytics.domain.frame.CovarianceMatrixArgs
//import com.intel.taproot.analytics.domain.frame.FrameEntity
//import com.intel.taproot.analytics.domain.schema.Column
//import com.intel.taproot.analytics.domain.schema.DataTypes.vector
//import org.apache.spark.mllib.linalg.Matrix
//import org.apache.spark.mllib.linalg.distributed.RowMatrix
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.catalyst.expressions.GenericRow
//
//// Implicits needed for JSON conversion
//import spray.json._
//import com.intel.taproot.analytics.domain.DomainJsonProtocol._
//
///**
// * Calculate k principal components for the specified columns of a frame
// * Parameters
// * ----------
// * columns : [ str | list of str ]
// *   The names of the column from which to compute the principal components.
// *   Names should refer to a single column of type vector, or two or more
// *   columns of numeric scalars.
// * k : Int
// *   The number of principal components to be computed
// *
// */
//
//class PrincipalComponentsPlugin extends SparkCommandPlugin[PrincipalComponentsArgs, FrameEntity]  {
//
//  /**
//   * The name of the command
//   */
//  override def name: String = "frame/principal_components"
//
//  /**
//   * Number of Spark jobs that get created by running this command
//   * (this configuration is used to prevent multiple progress bars in Python client)
//   */
//  override def numberOfJobs(arguments: PrincipalComponentsArgs)(implicit invocation: Invocation) = 7
//
//  /**
//   * Calculate covariance matrix for the specified columns
//   *
//   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
//   *                   that can be called to produce a SparkContext that can be used during this invocation
//   * @param arguments input specification for covariance matrix
//   * @return value of type declared as the Return type
//   */
//  override def execute(arguments: PrincipalComponentsArgs)(implicit invocation: Invocation): FrameEntity = {
//
//    val frame: SparkFrameData = resolve(arguments.frame)
//
//    // load frame as RDD
//    val frameRdd = frame.data
//    val frameSchema = frameRdd.frameSchema
//    validatePrincipalComponentsArgs(frameSchema, arguments)
//
//    // compute covariance
//    val outputColumnDataType = frameSchema.columnDataType(arguments.dataColumnNames(0))
//    val outputVectorLength: Option[Long] = outputColumnDataType match {
//      case vector(length) => Some(length)
//      case _ => None
//    }
//
//    def rowMatrix: RowMatrix = new RowMatrix(frameRdd.toVectorDenseRDD(arguments.dataColumnNames))
//    val principalComponents : Matrix = rowMatrix.computePrincipalComponents(arguments.k)
//    val projectedPrincipalComponents: RowMatrix = rowMatrix.multiply(principalComponents)
//
//    val vectorRdd = projectedPrincipalComponents.rows
//
//    //val f = vectorRdd.map( x => List(x).toArray)
//
//    val rowRdd = vectorRdd.map(x=>Row(x))
//
//    val outputSchema = getOutputSchema(arguments.dataColumnNames, outputVectorLength)
//    tryNew(CreateEntityArgs(description = Some("created by principal components"))) { newFrame: FrameMeta =>
//      if (arguments.matrixName.isDefined) {
//        engine.frames.renameFrame(newFrame.meta, FrameName.validate(arguments.matrixName.get))
//      }
//      save(new SparkFrameData(newFrame.meta, new FrameRdd(outputSchema, rowRdd)))
//    }.meta
//  }
//
//  // Validate input arguments
//  private def validatePrincipalComponentsArgs(frameSchema: Schema, arguments: PrincipalComponentsArgs): Unit = {
//    val dataColumnNames = arguments.dataColumnNames
//    if (dataColumnNames.size == 1) {
//      frameSchema.requireColumnIsType(dataColumnNames.toList(0), DataTypes.isVectorDataType)
//    }
//    else {
//      require(dataColumnNames.size >= 2, "single vector column, or two or more numeric columns required")
//      frameSchema.requireColumnsOfNumericPrimitives(dataColumnNames)
//    }
//    require(arguments.k >= 1, "k should be greater than equal to 1")
//  }
//
//  // Get output schema for covariance matrix
//  private def getOutputSchema(dataColumnNames: List[String], outputVectorLength: Option[Long] = None): FrameSchema = {
//    val outputColumns = outputVectorLength match {
//      case Some(length) => List(Column(dataColumnNames(0), DataTypes.vector(length)))
//      case _ => dataColumnNames.map(name => Column(name, DataTypes.float64)).toList
//    }
//    FrameSchema(outputColumns)
//  }
//
//
//
//}
