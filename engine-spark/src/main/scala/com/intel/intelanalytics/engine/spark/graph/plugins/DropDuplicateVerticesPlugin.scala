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

import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.engine.spark.frame.plugins.DropDuplicatesPlugin
import com.intel.intelanalytics.domain.FilterVerticesArgs
import com.intel.intelanalytics.domain.frame.{ DropDuplicatesArgs, FrameEntity }
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.ExecutionContext
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage, MiscFrameFunctions, LegacyFrameRDD }
import com.intel.intelanalytics.domain.graph.SeamlessGraphMeta
import org.apache.spark.SparkContext
import com.intel.intelanalytics.domain.schema.{ VertexSchema, DataTypes }
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

class DropDuplicateVerticesPlugin(graphStorage: SparkGraphStorage) extends SparkCommandPlugin[DropDuplicatesArgs, FrameEntity] {

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
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: DropDuplicatesArgs)(implicit invocation: Invocation) = 4

  /**
   * Plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DropDuplicatesArgs)(implicit invocation: Invocation): FrameEntity = {
    val frames = engine.frames.asInstanceOf[SparkFrameStorage]
    val vertexFrame = frames.expectFrame(arguments.frame.id)

    require(vertexFrame.isVertexFrame, "vertex frame is required")

    val seamlessGraph: SeamlessGraphMeta = graphStorage.expectSeamless(vertexFrame.graphId.get)
    val schema = vertexFrame.schema
    val rdd = frames.loadLegacyFrameRdd(sc, arguments.frame.id)
    val columnNames = arguments.unique_columns match {
      case Some(columns) => vertexFrame.schema.validateColumnsExist(columns.value).toList
      case None =>
        // _vid is always unique so don't include it
        vertexFrame.schema.columnNames.dropWhile(s => s == "_vid")
    }
    schema.validateColumnsExist(columnNames)
    val duplicatesRemoved: RDD[Array[Any]] = MiscFrameFunctions.removeDuplicatesByColumnNames(rdd, schema, columnNames)

    val label = schema.asInstanceOf[VertexSchema].label
    FilterVerticesFunctions.removeDanglingEdges(label, frames, seamlessGraph, sc, new LegacyFrameRDD(schema, duplicatesRemoved))

    // save results
    frames.saveLegacyFrame(vertexFrame.toReference, new LegacyFrameRDD(schema, duplicatesRemoved))

  }
}
