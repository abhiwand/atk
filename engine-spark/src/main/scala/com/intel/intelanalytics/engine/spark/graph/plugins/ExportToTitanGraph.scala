package com.intel.intelanalytics.engine.spark.graph.plugins

import com.intel.graphbuilder.driver.spark.titan.GraphBuilder
import com.intel.graphbuilder.elements.{ Edge => GBEdge, Vertex => GBVertex }
import com.intel.intelanalytics.domain.StorageFormats
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
  def toGBEdges(ctx: SparkContext, edges: List[DataFrame]): RDD[GBEdge] = {
    val gbEdges: RDD[GBEdge] = edges.foldLeft(ctx.parallelize[GBEdge](Nil))((gbFrame: RDD[GBEdge], frame: DataFrame) => {
      val edgeFrame: EdgeFrameRDD = new EdgeFrameRDD(frame.schema, frames.loadFrameRDD(ctx, frame))
      val gbEdgeFrame = edgeFrame.toGbEdgeRDD
      gbFrame.union(gbEdgeFrame)
    })
    gbEdges
  }

  def toGBVertices(ctx: SparkContext, vertices: List[DataFrame]): RDD[GBVertex] = {
    val gbVertices: RDD[GBVertex] = vertices.foldLeft(ctx.parallelize[GBVertex](Nil))((gbFrame: RDD[GBVertex], frame: DataFrame) => {
      val vertexFrame: VertexFrameRDD = new VertexFrameRDD(frame.schema, frames.loadFrameRDD(ctx, frame))
      val gbVertexFrame = vertexFrame.toGbVertexRDD
      gbFrame.union(gbVertexFrame)
    })
    gbVertices
  }

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

    val vertexRDD: RDD[GBVertex] = this.toGBVertices(invocation.sparkContext, seamlessGraph.vertexFrames)
    val edgeRDD: RDD[GBEdge] = this.toGBEdges(invocation.sparkContext, seamlessGraph.edgeFrames)

    val emptyGraphLoad = new GraphLoad(new GraphReference(titanGraph.id), List(new FrameRule(null, List(), List())))
    val gbConfigFactory = new GraphBuilderConfigFactory(new Schema(List()), emptyGraphLoad, titanGraph)
    val graphBuilder = new GraphBuilder(gbConfigFactory.graphConfig)
    graphBuilder.buildGraphWithSpark(vertexRDD, edgeRDD)

    titanGraph
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
}
