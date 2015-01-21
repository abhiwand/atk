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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameName, DataFrameTemplate, JoinArgs, FrameEntity }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{ Schema, SchemaUtil }
import com.intel.intelanalytics.engine.Rows
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage, LegacyFrameRDD, RDDJoinParam, MiscFrameFunctions }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.concurrent.{ Await, ExecutionContext }
import com.intel.intelanalytics.domain.CreateEntityArgs

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Join two data frames (similar to SQL JOIN)
 */
class JoinPlugin(frames: SparkFrameStorage) extends SparkCommandPlugin[JoinArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/join"

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
    // dependencies (later to be replaced with dependency injection)
    val ctx = sc

    val originalColumns = arguments.frames.map {
      frame =>
        {
          val frameEntity = frames.expectFrame(frame._1)
          frameEntity.schema.columnTuples
        }
    }

    val leftColumns: List[(String, DataType)] = originalColumns(0)
    val rightColumns: List[(String, DataType)] = originalColumns(1)
    val allColumns = SchemaUtil.resolveSchemaNamingConflicts(leftColumns, rightColumns)
    val newJoinFrame = frames.create(CreateEntityArgs(name = arguments.name, description = Some("created from join operation")))

    //first validate join columns are valid
    val leftOn: String = arguments.frames(0)._2
    val rightOn: String = arguments.frames(1)._2

    val leftSchema = Schema.fromTuples(leftColumns)
    val rightSchema = Schema.fromTuples(rightColumns)

    require(leftSchema.columnIndex(leftOn) != -1, s"column $leftOn is invalid")
    require(rightSchema.columnIndex(rightOn) != -1, s"column $rightOn is invalid")

    val pairRdds = createPairRddForJoin(arguments, ctx)

    val joinResultRDD = MiscFrameFunctions.joinRDDs(RDDJoinParam(pairRdds(0), leftColumns.length),
      RDDJoinParam(pairRdds(1), rightColumns.length),
      arguments.how)

    frames.saveLegacyFrame(newJoinFrame.toReference, new LegacyFrameRDD(Schema.fromTuples(allColumns), joinResultRDD))
  }

  def createPairRddForJoin(arguments: JoinArgs, ctx: SparkContext)(implicit invocation: Invocation): List[RDD[(Any, Array[Any])]] = {
    val tupleRddColumnIndex: List[(RDD[Rows.Row], Int)] = arguments.frames.map {
      frame =>
        {
          val frameEntity = frames.lookup(frame._1).getOrElse(
            throw new IllegalArgumentException(s"No such data frame"))

          val frameSchema = frameEntity.schema
          val rdd = frames.loadLegacyFrameRdd(ctx, frame._1)
          val columnIndex = frameSchema.columnIndex(frame._2)
          (rdd, columnIndex)
        }
    }

    val pairRdds = tupleRddColumnIndex.map {
      case (rdd, columnIndex) =>
        rdd.map(p => MiscFrameFunctions.createKeyValuePairFromRow(p, Seq(columnIndex))).map {
          case (keyColumns, data) => (keyColumns(0), data)
        }
    }

    pairRdds
  }
}
