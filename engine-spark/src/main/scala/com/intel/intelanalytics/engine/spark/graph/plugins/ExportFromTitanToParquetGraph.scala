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

import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphNoArgs }
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.domain.schema.{ EdgeSchema, Schema, VertexSchema }
import com.intel.intelanalytics.engine.spark.frame.{ LegacyFrameRDD, SparkFrameStorage }
import com.intel.intelanalytics.domain.schema.DataTypes.int64
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.graphbuilder.elements.{ Property, Vertex }
// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

class ExportFromTitanToParquetGraph(frames: SparkFrameStorage, graphs: SparkGraphStorage) extends SparkCommandPlugin[GraphNoArgs, DataFrame] {
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
  override def name: String = "graph:titan/export_to_parquet"

  /**
   * Plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: GraphNoArgs)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    val ctx = invocation.sparkContext
    val vertices = graphs.loadGbVertices(ctx, arguments.graph.id)
    vertices.cache()

    val vertexLabels = vertices.map(v => {
      val label = v.gbId.key
      label
    }).distinct().collect()

    val graph = graphs.createGraph(GraphTemplate("name", "ia/frame"))

    vertexLabels.foreach(label => {
      graphs.defineVertexType(graph.id, VertexSchema(label, None))
    })

    val seemless = graphs.expectSeamless(graph.id)

    seemless.vertexFrames.foreach(vertexFrame => {
      val label = vertexFrame.schema.vertexSchema.get.label
      val typeVertex: RDD[Vertex] = vertices.filter(v => v.gbId.key.equalsIgnoreCase(label))
      val columns = typeVertex.take(1).apply(0).fullProperties.map(p => p.key).toList

      val sourceRdd: RDD[Row] = typeVertex.map(v => ExportFromTitanToParquetGraph.getPropertiesValueByColumns(columns, v.fullProperties))

      //also need to include the property schema
      val schema = new Schema(List((label, int64)))
      val source = new LegacyFrameRDD(schema, sourceRdd).toFrameRDD()
      AddVerticesPlugin.addVertices(ctx, source, vertexFrame.frameReference, columns, label, frames, graphs)
    })

    vertices.unpersist()

    val edges = graphs.loadGbEdges(ctx, arguments.graph.id)
    val edgeDefinitions = edges.map(e => {
      EdgeSchema(e.label, e.headVertexGbId.key, e.tailVertexGbId.key)
    }).distinct().collect()

    edgeDefinitions.foreach(edgeDef => {
      graphs.defineEdgeType(graph.id, edgeDef)
    })

    seemless.edgeFrames.foreach(edgeFrame => {

    })

    null
  }
}

object ExportFromTitanToParquetGraph {

  def getPropertiesValueByColumns(columns: List[String], properties: Set[Property]): Array[Any] = {
    val mapping = properties.map(p => p.key -> p.value).toMap
    columns.map(c => mapping(c)).toArray
  }
}

