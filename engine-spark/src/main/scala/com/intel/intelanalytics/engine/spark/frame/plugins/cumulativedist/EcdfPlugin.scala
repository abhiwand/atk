//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine.spark.frame.plugins.cumulativedist

import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema, Column }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData, SparkFrameStorage, LegacyFrameRDD }
import com.intel.intelanalytics.domain.schema.{ FrameSchema, DataTypes, Schema, Column }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.domain.CreateEntityArgs

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Empirical Cumulative Distribution for a column
 */
class EcdfPlugin extends SparkCommandPlugin[EcdfArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/ecdf"

  override def numberOfJobs(arguments: EcdfArgs)(implicit invocation: Invocation) = 6

  /**
   * Empirical Cumulative Distribution for a column
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: EcdfArgs)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)

    // validate arguments
    val frame: SparkFrameData = resolve(arguments.frame)
    val sampleColumn = frame.meta.schema.column(arguments.column)
    require(sampleColumn.dataType.isNumerical, s"Invalid column ${sampleColumn.name} for ECDF.  Expected a numeric data type, but got ${sampleColumn.dataType}.")
    val ecdfSchema = FrameSchema(List(sampleColumn.copy(), Column(sampleColumn.name + "_ECDF", DataTypes.float64)))

    // run the operation
    tryNew(CreateEntityArgs(description = Some("created by ECDF operation"))) { ecdfFrame: FrameMeta =>
      if (arguments.resultFrameName.isDefined) {
        engine.frames.renameFrame(ecdfFrame.meta, FrameName.validate(arguments.resultFrameName.get))
      }
      val rdd = frame.data.toLegacyFrameRDD
      val ecdfRdd = CumulativeDistFunctions.ecdf(rdd, sampleColumn)
      save(new SparkFrameData(ecdfFrame.meta.withSchema(ecdfSchema),
        new LegacyFrameRDD(ecdfSchema, ecdfRdd).toFrameRDD()))
    }.meta
  }
}
