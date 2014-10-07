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
import com.intel.intelanalytics.domain.frame.{ DataFrameTemplate, FrameJoin, DataFrame }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{ Schema, SchemaUtil }
import com.intel.intelanalytics.engine.Rows
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage, FrameRDD, RDDJoinParam, MiscFrameFunctions }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.concurrent.{ Await, ExecutionContext }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Join two data frames (similar to SQL JOIN)
 */
class JoinPlugin(frames: SparkFrameStorage) extends SparkCommandPlugin[FrameJoin, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/join"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = None

  /**
   * Join two data frames (similar to SQL JOIN)
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments parameter contains information for the join operation (user supplied arguments to running this plugin)
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: FrameJoin)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val ctx = invocation.sparkContext

    val originalColumns = arguments.frames.map {
      frame =>
        {
          val frameMeta = frames.expectFrame(frame._1)
          frameMeta.schema.columns
        }
    }

    val leftColumns: List[(String, DataType)] = originalColumns(0)
    val rightColumns: List[(String, DataType)] = originalColumns(1)
    val allColumns = SchemaUtil.resolveSchemaNamingConflicts(leftColumns, rightColumns)

    val newJoinFrame = frames.create(DataFrameTemplate(arguments.name, None))

    //first validate join columns are valid
    val leftOn: String = arguments.frames(0)._2
    val rightOn: String = arguments.frames(1)._2

    val leftSchema = Schema(leftColumns)
    val rightSchema = Schema(rightColumns)

    require(leftSchema.columnIndex(leftOn) != -1, s"column $leftOn is invalid")
    require(rightSchema.columnIndex(rightOn) != -1, s"column $rightOn is invalid")

    val pairRdds = createPairRddForJoin(arguments, ctx)

    val joinResultRDD = MiscFrameFunctions.joinRDDs(RDDJoinParam(pairRdds(0), leftColumns.length),
      RDDJoinParam(pairRdds(1), rightColumns.length),
      arguments.how)

    val joinRowCount = joinResultRDD.count()
    frames.saveFrame(newJoinFrame, new FrameRDD(new Schema(allColumns), joinResultRDD), Some(joinRowCount))
  }

  def createPairRddForJoin(arguments: FrameJoin, ctx: SparkContext): List[RDD[(Any, Array[Any])]] = {
    val tupleRddColumnIndex: List[(RDD[Rows.Row], Int)] = arguments.frames.map {
      frame =>
        {
          val frameMeta = frames.lookup(frame._1).getOrElse(
            throw new IllegalArgumentException(s"No such data frame"))

          val frameSchema = frameMeta.schema
          val rdd = frames.loadFrameRdd(ctx, frame._1)
          val columnIndex = frameSchema.columnIndex(frame._2)
          (rdd, columnIndex)
        }
    }

    val pairRdds = tupleRddColumnIndex.map {
      t =>
        val rdd = t._1
        val columnIndex = t._2
        rdd.map(p => MiscFrameFunctions.createKeyValuePairFromRow(p, Seq(columnIndex))).map { case (keyColumns, data) => (keyColumns(0), data) }
    }

    pairRdds
  }
}
