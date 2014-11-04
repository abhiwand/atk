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

package com.intel.intelanalytics.engine.spark.graph.plugins

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.domain.FilterVertexRows
import com.intel.intelanalytics.domain.frame.{ DropDuplicates, DataFrame }
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.ExecutionContext
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark.frame.{ MiscFrameFunctions, LegacyFrameRDD }
import com.intel.intelanalytics.domain.graph.SeamlessGraphMeta
import org.apache.spark.SparkContext
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

class DropDuplicateVerticesPlugin(graphStorage: SparkGraphStorage) extends SparkCommandPlugin[DropDuplicates, DataFrame] {
  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   *
   * The colon ":" is used to to indicate command destination base classes, default classes or classes of a
   * specific storage type:
   *
   * - graph:titan means command is loaded into class TitanGraph
   * - graph: means command is loaded into class Graph, our default type which will be the Parquet-backed graph
   * - graph would mean command is loaded into class BaseGraph, which applies to all graph classes
   * - frame: and means command is loaded in class Frame.  Example: "frame:/assign_sample"
   * - model:logistic_regression  means command is loaded into class LogisticRegressionModel
   */
  override def name: String = "frame:vertex/drop_duplicates"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  /**
   * 20141104 Mark Aldrich: Changed to match other plugin functions
   * override def doc: Option[CommandDoc] = Some(CommandDoc("Remove duplicate vertex rows.", Some("""
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Remove duplicate vertex rows.",
    extendedSummary = Some("""
                           |    Extended Summary
                           |    ----------------
                           |    Remove duplicate vertex rows, keeping only one vertex row per uniqueness
                           |    criteria match
                           |
                           |    Parameters
                           |    ----------
                           |    columns:[str | list of str]
                           |        Column name(s) to identify duplicates.
                           |        If empty, the function will remove duplicates that have the whole row of
                           |        data identical (not including the _vid column that is already unique per
                           |        row).
                           |
                           |    Examples
                           |    --------
                           |    Remove any rows that have the same data in column *b* as a previously
                           |    checked row::
                           |
                           |        my_frame.drop_duplicates("b")
                           |
                           |    The result is a frame with unique values in column *b*.
                           |
                           |    Remove any rows that have the same data in columns *a* and *b* as a
                           |    previously checked row::
                           |
                           |        my_frame.drop_duplicates([ "a", "b"] )
                           |
                           |    The result is a frame with unique values for the combination of columns
                           |    *a* and *b*.
                           |
                           |    Remove any rows that have the whole row identical::
                           |
                           |        my_frame.drop_duplicates()
                           |
                           |    The result is a frame where something is different in every row from every
                           |    other row.
                           |    Each row is unique.
                           |
                           |    .versionadded :: 0.8
                           |
                            """.stripMargin)))

  /**
   * Plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: DropDuplicates)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    val frames = invocation.engine.frames
    val vertexFrame = frames.expectFrame(arguments.frame.id)

    vertexFrame.graphId match {
      case Some(graphId) => {
        val seamlessGraph: SeamlessGraphMeta = graphStorage.expectSeamless(graphId)

        val ctx: SparkContext = invocation.sparkContext
        val schema = vertexFrame.schema
        val rdd = frames.loadLegacyFrameRdd(ctx, arguments.frame.id)
        val columnNames = arguments.unique_columns match {
          case Some(columns) => vertexFrame.schema.validateColumnsExist(columns.value).toList
          case None => vertexFrame.schema.columnNames
        }
        schema.validateColumnsExist(columnNames)
        val duplicatesRemoved: RDD[Array[Any]] = MiscFrameFunctions.removeDuplicatesByColumnNames(rdd, schema, columnNames)

        val label = schema.vertexSchema.get.label
        FilterVerticesFunctions.removeDanglingEdges(label, frames, seamlessGraph, ctx, new LegacyFrameRDD(schema, duplicatesRemoved))

        val rowCount = duplicatesRemoved.count()
        // save results
        frames.saveLegacyFrame(vertexFrame, new LegacyFrameRDD(schema, duplicatesRemoved), Some(rowCount))
      }
      case _ => null //abort
    }
  }
}
