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

package com.intel.intelanalytics.engine.spark.frame.plugins.load

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.domain.frame.load.Load
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal

import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

import scala.concurrent.ExecutionContext

/**
 * Parsing data to load and append to data frames
 */
class LoadFramePlugin extends SparkCommandPlugin[Load, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "dataframe/load"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = None

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(load: Load) = 8

  /**
   * Parsing data to load and append to data frames
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: Load)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val sparkAutoPartitioner = invocation.engine.sparkAutoPartitioner
    val fsRoot = invocation.engine.fsRoot
    val ctx = invocation.sparkContext

    // validate arguments
    val frameId = arguments.destination.id
    val destinationFrame = frames.expectFrame(frameId)

    // run the operation
    if (arguments.source.isFrame) {
      // load data from an existing frame and add its data onto the target frame
      val additionalData = frames.loadFrameRdd(ctx, frames.expectFrame(arguments.source.uri.toInt))
      unionAndSave(invocation, destinationFrame, additionalData)
    }
    else if (arguments.source.isFile) {
      val parser = arguments.source.parser.get
      val partitions = sparkAutoPartitioner.partitionsForFile(arguments.source.uri)
      val parseResult = LoadRDDFunctions.loadAndParseLines(ctx, fsRoot + "/" + arguments.source.uri, parser, partitions)

      // parse failures go to their own data frame
      if (parseResult.errorLines.count() > 0) {
        val errorFrame = frames.lookupOrCreateErrorFrame(destinationFrame)
        unionAndSave(invocation, errorFrame, parseResult.errorLines)
      }

      // successfully parsed lines get added to the destination frame
      unionAndSave(invocation, destinationFrame, parseResult.parsedLines)
    }
    else {
      throw new IllegalArgumentException("Unsupported load source: " + arguments.source.source_type)
    }
  }

  /**
   * Union the additionalData onto the end of the existingFrame
   * @param existingFrame the target DataFrame that may or may not already have data
   * @param additionalData the data to add to the existingFrame
   * @return the frame with updated schema
   */
  private def unionAndSave(invocation: SparkInvocation, existingFrame: DataFrame, additionalData: FrameRDD): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    val existingRdd = frames.loadFrameRdd(ctx, existingFrame)
    val unionedRdd = existingRdd.union(additionalData)
    val rowCount = unionedRdd.count()
    frames.saveFrame(existingFrame, unionedRdd, Some(rowCount))
  }

}
