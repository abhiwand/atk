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

package com.intel.intelanalytics.engine.spark.frame.plugins.load

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameReference, FrameEntity }
import com.intel.intelanalytics.domain.frame.load.LoadFrameArgs
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.LegacyFrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.frame.FrameRDD

import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Parsing data to load and append to data frames
 */
class LoadFramePlugin extends SparkCommandPlugin[LoadFrameArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graph/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/load"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(load: LoadFrameArgs)(implicit invocation: Invocation) = 8

  /**
   * Parsing data to load and append to data frames
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: LoadFrameArgs)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames
    val sparkAutoPartitioner = engine.sparkAutoPartitioner
    val fsRoot = engine.fsRoot
    val ctx = sc

    // validate arguments
    val frameRef = arguments.destination
    val destinationFrame = frames.expectFrame(frameRef)

    // run the operation
    if (arguments.source.isFrame) {
      // load data from an existing frame and add its data onto the target frame
      val additionalData = frames.loadFrameData(ctx, frames.expectFrame(FrameReference(arguments.source.uri.toInt)))
      unionAndSave(destinationFrame, additionalData)
    }
    else if (arguments.source.isFile || arguments.source.isMultilineFile) {
      val partitions = sparkAutoPartitioner.partitionsForFile(arguments.source.uri)
      val parseResult = LoadRDDFunctions.loadAndParseLines(ctx, fsRoot + "/" + arguments.source.uri,
        null, partitions, arguments.source.startTag, arguments.source.endTag, arguments.source.sourceType.contains("xml"))
      unionAndSave(destinationFrame, parseResult.parsedLines)

    }
    else if (arguments.source.isFieldDelimited || arguments.source.isClientData) {
      val parser = arguments.source.parser.get

      val parseResult = if (arguments.source.isFieldDelimited) {
        val partitions = sparkAutoPartitioner.partitionsForFile(arguments.source.uri)
        LoadRDDFunctions.loadAndParseLines(ctx, fsRoot + "/" + arguments.source.uri, parser, partitions)
      }
      else {
        val data = arguments.source.data.get
        LoadRDDFunctions.loadAndParseData(ctx, data, parser)
      }
      // parse failures go to their own data frame
      val updatedFrame = if (parseResult.errorLines.count() > 0) {
        val (updated, errorFrame) = frames.lookupOrCreateErrorFrame(destinationFrame)
        unionAndSave(errorFrame, parseResult.errorLines)
        updated
      }
      else {
        destinationFrame
      }

      // successfully parsed lines get added to the destination frame
      unionAndSave(destinationFrame, parseResult.parsedLines.dropIgnoreColumns())
    }

    else {
      throw new IllegalArgumentException("Unsupported load source: " + arguments.source.sourceType)
    }
  }

  /**
   * Union the additionalData onto the end of the existingFrame
   * @param existingFrame the target DataFrame that may or may not already have data
   * @param additionalData the data to add to the existingFrame
   * @return the frame with updated schema
   */
  private def unionAndSave(existingFrame: FrameEntity, additionalData: FrameRDD)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames
    val ctx = sc

    val existingRdd = frames.loadFrameData(ctx, existingFrame)
    val unionedRdd = existingRdd.union(additionalData)
    frames.saveFrameData(existingFrame.toReference, unionedRdd)
  }

}
