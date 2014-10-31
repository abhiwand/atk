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

import com.intel.graphbuilder.driver.spark.titan.GraphBuilder
import com.intel.graphbuilder.elements.{ Edge => GBEdge, Vertex => GBVertex }
import com.intel.intelanalytics.domain.StorageFormats
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.domain.graph._
import com.intel.intelanalytics.domain.graph.construction.FrameRule
import com.intel.intelanalytics.domain.schema.Schema
import com.intel.intelanalytics.engine.spark.frame.SparkFrameStorage
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.SparkContext
import org.apache.spark.ia.graph.{ EdgeFrameRDD, VertexFrameRDD }
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark.graph.GraphBuilderConfigFactory

import scala.concurrent.ExecutionContext
// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

class ExportToTitanGraph(frames: SparkFrameStorage, graphs: SparkGraphStorage) extends SparkCommandPlugin[ExportGraph, Graph] {


  /**
   * Plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: ExportGraph)(implicit user: UserPrincipal, executionContext: ExecutionContext): Graph = {
    val seamlessGraph: SeamlessGraphMeta = graphs.expectSeamless(arguments.graph.id)
    val titanGraph: Graph = graphs.createGraph(
      new GraphTemplate(
        arguments.newGraphName match {
          case Some(name) => name
          case None => frames.generateFrameName(prefix = "titan_graph")
        },
        StorageFormats.HBaseTitan))
    val ctx = invocation.sparkContext

    val vertexRDD: RDD[GBVertex] = this.toGBVertices(ctx,
      seamlessGraph.vertexFrames.map(frame => new VertexFrameRDD(frame.schema, frames.loadFrameRDD(ctx, frame))))
    val edgeRDD: RDD[GBEdge] = this.toGBEdges(ctx,
      seamlessGraph.edgeFrames.map(frame => new EdgeFrameRDD(frame.schema, frames.loadFrameRDD(ctx, frame))))

    loadTitanGraph(titanGraph, vertexRDD, edgeRDD)
    titanGraph
  }


  /**
   * convert a list of EdgeFrameRDDs to a single RDD of GBEdge objects
   * @param ctx sparkContext
   * @param edges  list of all Edges found in a Seamless Graph
   * @return  RDD[GBEdge] object ready for conversion to titan
   */
  def toGBEdges(ctx: SparkContext, edges: List[EdgeFrameRDD]): RDD[GBEdge] = {
    val gbEdges: RDD[GBEdge] = edges.foldLeft(ctx.parallelize[GBEdge](Nil))((gbFrame: RDD[GBEdge], edgeFrame: EdgeFrameRDD) => {
      val gbEdgeFrame = edgeFrame.toGbEdgeRDD
      gbFrame.union(gbEdgeFrame)
    })
    gbEdges
  }

  /**
   * convert a list of VertexFrameRDDs to a single RDD of GBVertex objects
   * @param ctx sparkContext
   * @param vertices  list of all Vertices found in a Seamless Graph
   * @return  RDD[GBVertex] object ready for conversion to titan
   */
  def toGBVertices(ctx: SparkContext, vertices: List[VertexFrameRDD]): RDD[GBVertex] = {
    val gbVertices: RDD[GBVertex] = vertices.foldLeft(ctx.parallelize[GBVertex](Nil))((gbFrame: RDD[GBVertex], vertexFrame: VertexFrameRDD) => {
      val gbVertexFrame = vertexFrame.toGbVertexRDD
      gbFrame.union(gbVertexFrame)
    })
    gbVertices
  }

  /**
   * load the vertices and edges into a titan graph
   * @param titanGraph newly created titan graph that will be loaded with the Seamless Graph data
   * @param vertexRDD RDD of GBVertex objects found in seamless graph
   * @param edgeRDD  RDD of GBVertex objects found in a seamless graph
   */
  def loadTitanGraph(titanGraph: Graph, vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge]) {
    val emptyGraphLoad = new GraphLoad(new GraphReference(titanGraph.id), List(new FrameRule(null, List(), List())))
    val gbConfigFactory = new GraphBuilderConfigFactory(new Schema(List()), emptyGraphLoad, titanGraph)
    val graphBuilder = new GraphBuilder(gbConfigFactory.graphConfig)
    graphBuilder.buildGraphWithSpark(vertexRDD, edgeRDD)
  }

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
  override def name: String = "graph:/export_to_titan"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Convert to TitanGraph",
    extendedSummary = Some("""
    Convert this Graph into a TitanGraph object. This will be a new graph backed by Titan with all of the data found in this graph

    Parameters
    ----------
    new_graph_name: str
      the name of the new graph. This is optional. If omitted a name will be generated.

    Examples
    --------
       |graph = ia.get_graph("my_graph")
       |titan_graph = graph.export_to_titan("titan_graph") """)))

}
