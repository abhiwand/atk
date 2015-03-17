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
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import org.apache.spark.frame.FrameRdd
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
    val frames = engine.frames
    val frame = frames.expectFrame(arguments.frame)
    val frameSchema = frame.schema

    // validate arguments    
    frameSchema.validateColumnsExist(arguments.leftColumnNames)
    frameSchema.validateColumnsExist(arguments.rightColumnNames)
    require(!frameSchema.hasColumn(arguments.dotProductColumnName), s"Column name already exists: ${arguments.dotProductColumnName}")

    // run the operation
    val frameRdd = frames.loadFrameData(sc, frame)
    val dotProductRdd = DotProductFunctions.dotProduct(frameRdd, arguments.leftColumnNames, arguments.rightColumnNames,
      arguments.defaultLeftValues, arguments.defaultRightValues)

    // save results
    val updatedSchema = frameSchema.addColumn(arguments.dotProductColumnName, DataTypes.float64)
    frames.saveFrameData(frame.toReference, new FrameRdd(updatedSchema, dotProductRdd))
  }
}
