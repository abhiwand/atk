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

package com.intel.intelanalytics.engine.spark.frame.plugins.load

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameReference, FrameEntity }
import com.intel.intelanalytics.domain.frame.load.LoadFrameArgs
import com.intel.intelanalytics.domain.schema.DataTypes.{ float32, float64, int64, int32 }
import com.intel.intelanalytics.domain.schema.{ FrameSchema, DataTypes, Column }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.LegacyFrameRdd
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.types._

import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

import scala.collection.mutable.ListBuffer

/**
 * Parsing data to load and append to data frames
 */
@PluginDoc(oneLine = "",
  extended = "",
  returns = "")
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
    def getAbsolutePath(s: String): String = engine.frames.frameFileStorage.hdfs.absolutePath(s).toString

    // validate arguments
    val frameRef = arguments.destination
    val destinationFrame = frames.expectFrame(frameRef)

    // run the operation
    if (arguments.source.isFrame) {
      // load data from an existing frame and add its data onto the target frame
      val additionalData = frames.loadFrameData(sc, frames.expectFrame(FrameReference(arguments.source.uri.toInt)))
      unionAndSave(destinationFrame, additionalData)
    }
    else if (arguments.source.isFile || arguments.source.isMultilineFile) {
      val filePath = getAbsolutePath(arguments.source.uri)
      val partitions = sparkAutoPartitioner.partitionsForFile(filePath)
      val parseResult = LoadRddFunctions.loadAndParseLines(sc, filePath,
        null, partitions, arguments.source.startTag, arguments.source.endTag, arguments.source.sourceType.contains("xml"))
      unionAndSave(destinationFrame, parseResult.parsedLines)

    }
    else if (arguments.source.isHiveDb) {
      val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
      val rdd = sqlContext.sql(arguments.source.uri) //use URI
      unionAndSave(destinationFrame, LoadRddFunctions.convertHiveRddToFrameRdd(rdd))
    }
    else if (arguments.source.isFieldDelimited || arguments.source.isClientData) {
      val parser = arguments.source.parser.get
      val filePath = getAbsolutePath(arguments.source.uri)
      val parseResult = if (arguments.source.isFieldDelimited) {
        val partitions = sparkAutoPartitioner.partitionsForFile(filePath)
        LoadRddFunctions.loadAndParseLines(sc, filePath, parser, partitions)
      }
      else {
        val data = arguments.source.data.get
        LoadRddFunctions.loadAndParseData(sc, data, parser)
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
  private def unionAndSave(existingFrame: FrameEntity, additionalData: FrameRdd)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    val existingRdd = frames.loadFrameData(sc, existingFrame)
    val unionedRdd = existingRdd.union(additionalData)
    frames.saveFrameData(existingFrame.toReference, unionedRdd)
  }

}
