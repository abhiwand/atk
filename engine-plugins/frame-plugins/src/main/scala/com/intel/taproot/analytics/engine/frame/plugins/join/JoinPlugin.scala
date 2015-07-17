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

package com.intel.taproot.analytics.engine.spark.frame.plugins.join

import com.intel.taproot.analytics.domain.CreateEntityArgs
import com.intel.taproot.analytics.domain.DomainJsonProtocol._
import com.intel.taproot.analytics.domain.frame.FrameEntity
import com.intel.taproot.analytics.domain.schema.{ FrameSchema, Schema }
import com.intel.taproot.analytics.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.spark.EngineConfig
import com.intel.taproot.analytics.engine.spark.frame._
import com.intel.taproot.analytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd

/** Json conversion for arguments and return value case classes */
object JoinJsonFormat {
  implicit val JoinFrameFormat = jsonFormat2(JoinFrameArgs)
  implicit val JoinArgsFormat = jsonFormat4(JoinArgs)
}

import JoinJsonFormat._

/**
 * Join two data frames (similar to SQL JOIN)
 */
@PluginDoc(oneLine = "Join two data frames (similar to SQL JOIN).",
  extended = "",
  returns = "")
class JoinPlugin extends SparkCommandPlugin[JoinArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/join"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  override def numberOfJobs(arguments: JoinArgs)(implicit invocation: Invocation): Int = 2

  /**
   * Join two data frames (similar to SQL JOIN)
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments parameter contains information for the join operation (user supplied arguments to running this plugin)
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: JoinArgs)(implicit invocation: Invocation): FrameEntity = {
    val leftFrame: SparkFrame = arguments.leftFrame.frame
    val rightFrame: SparkFrame = arguments.rightFrame.frame

    //first validate join columns are valid
    leftFrame.schema.validateColumnsExist(List(arguments.leftFrame.joinColumn))
    rightFrame.schema.validateColumnsExist(List(arguments.rightFrame.joinColumn))

    // Get estimated size of frame to determine whether to use a broadcast join
    val broadcastJoinThreshold = EngineConfig.broadcastJoinThreshold

    val joinResultRDD = JoinRddFunctions.joinRDDs(
      createRDDJoinParam(leftFrame, arguments.leftFrame.joinColumn, broadcastJoinThreshold),
      createRDDJoinParam(rightFrame, arguments.rightFrame.joinColumn, broadcastJoinThreshold),
      arguments.how, broadcastJoinThreshold
    )

    val allColumns = Schema.join(leftFrame.schema.columns, rightFrame.schema.columns)
    val newJoinSchema = FrameSchema(allColumns)

    val joinedFrame = new FrameRdd(newJoinSchema, joinResultRDD)

    engine.frames.tryNewFrame(CreateEntityArgs(name = arguments.name, description = Some("created from join operation"))) {
      newFrame => newFrame.save(joinedFrame)
    }
  }

  //Create parameters for join
  private def createRDDJoinParam(frame: SparkFrame,
                                 joinColumn: String,
                                 broadcastJoinThreshold: Long)(implicit invocation: Invocation): RddJoinParam = {
    val frameSize = if (broadcastJoinThreshold > 0) frame.sizeInBytes else None
    val pairRdd = frame.rdd.keyByRows(row => row.value(joinColumn))
    RddJoinParam(pairRdd, frame.schema.columns.length, frameSize)
  }
}
