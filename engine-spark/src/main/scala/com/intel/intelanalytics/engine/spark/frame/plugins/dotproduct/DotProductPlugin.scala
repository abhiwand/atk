package com.intel.intelanalytics.engine.spark.frame.plugins.dotproduct

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

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
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

    val leftVectorSize = getVectorSize(frameRdd, arguments.leftColumnNames)
    val rightVectorSize = getVectorSize(frameRdd, arguments.rightColumnNames)

    require(leftVectorSize == rightVectorSize,
      "number of elements in left columns should equal number in right columns")
    require(arguments.defaultLeftValues.isEmpty || arguments.defaultLeftValues.get.size == leftVectorSize,
      "size of default left values should match number of elements in left columns")
    require(arguments.defaultRightValues.isEmpty || arguments.defaultRightValues.get.size == leftVectorSize,
      "size of default right values should match number of elements in right columns")
    require(!frameSchema.hasColumn(arguments.dotProductColumnName),
      s"Column name already exists: ${arguments.dotProductColumnName}")
  }

  // Get the size of the column vector
  private def getVectorSize(frameRdd: FrameRdd, columnNames: List[String]): Int = {
    val frameSchema = frameRdd.frameSchema
    if (columnNames.size == 1) {
      frameSchema.requireColumnIsType(columnNames(0), DataTypes.vector)
      //TODO: Remove once vector data type supports size
      val firstDefinedValue = frameRdd.mapRows(row => row.value(columnNames(0))).filter(_ != null).take(1)
      if (firstDefinedValue.nonEmpty) DataTypes.toVector(firstDefinedValue(0)).size else 1
    }
    else {
      require(columnNames.size >= 1, "single vector column, or one or more numeric columns required")
      frameSchema.requireColumnsOfNumericPrimitives(columnNames)
      columnNames.size
    }
  }
}
