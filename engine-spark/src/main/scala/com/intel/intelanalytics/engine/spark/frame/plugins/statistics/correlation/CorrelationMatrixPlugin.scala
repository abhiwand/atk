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
import org.apache.spark.frame.FrameRDD
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

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate correlation matrix for the specified columns
 */
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

    val inputDataColumnNamesAndTypes: List[Column] = arguments.dataColumnNames.map({ name => Column(name, DataTypes.float64) }).toList
    val correlationRDD = Correlation.correlationMatrix(rdd, arguments.dataColumnNames)

    val schema = FrameSchema(inputDataColumnNamesAndTypes)
    tryNew(CreateEntityArgs(description = Some("created by correlation matrix command"))) { newFrame: FrameMeta =>
      if (arguments.matrixName.isDefined) {
        engine.frames.renameFrame(newFrame.meta, FrameName.validate(arguments.matrixName.get))
      }
      save(new SparkFrameData(newFrame.meta, new FrameRDD(schema, correlationRDD)))
    }.meta
  }
}
