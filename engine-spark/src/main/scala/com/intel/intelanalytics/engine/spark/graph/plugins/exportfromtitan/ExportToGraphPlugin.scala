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

package com.intel.intelanalytics.engine.spark.graph.plugins.exportfromtitan

import com.intel.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import com.intel.intelanalytics.domain.graph._
import com.intel.intelanalytics.domain.schema.DataTypes._
import com.intel.intelanalytics.domain.schema.{ Column, VertexSchema, _ }
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ LegacyFrameRdd, SparkFrameStorage }
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.Vertex
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.ia.graph.{ VertexWrapper, EdgeWrapper }
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

import scala.collection.JavaConversions._

import org.apache.spark.SparkContext._

import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._

/**
 * Holds an edge plus src and dest vertex labels.
 */
case class EdgeHolder(edge: GBEdge, srcLabel: String, destLabel: String)

/**
 * holds 3 labels
 * @param edgeLabel the label of the edge
 * @param srcLabel the label (or type) of the source vertex
 * @param destLabel the label (or type) of the destination vertex
 */
case class LabelTriplet(edgeLabel: String, srcLabel: String, destLabel: String)

/**
 * Export from ia.TitanGraph to ia.Graph
 */
class ExportToGraphPlugin(frames: SparkFrameStorage, graphs: SparkGraphStorage) extends SparkCommandPlugin[GraphNoArgs, GraphEntity] {

  override def name: String = "graph:titan/export_to_graph"

  override def numberOfJobs(arguments: GraphNoArgs)(implicit invocation: Invocation): Int = {
    // NOTE: there isn't really a good way to set this number for this plugin
    8
  }

  /**
   * Plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: GraphNoArgs)(implicit invocation: Invocation): GraphEntity = {

    val titanGraphEntity = graphs.expectGraph(arguments.graph)
    require(titanGraphEntity.isTitan, "Titan graph is required for this operation")

    val (vertices, edges) = graphs.loadFromTitan(sc, titanGraphEntity)
    vertices.cache()
    edges.cache()

    // We're going to re-use the physical ids that were present in Titan for _vid and _eid
    val maxVertexId = vertices.map(v => v.physicalId.asInstanceOf[Long]).reduce((a, b) => Math.max(a, b))
    val maxEdgeId = edges.flatMap(e => e.eid).reduce((a, b) => Math.max(a, b))

    // unique indices should exist on Vertex properties that were a user-defined Vertex ID
    val indexNames: List[String] = ExportToGraphPlugin.uniqueVertexPropertyIndices(graphs, titanGraphEntity.toReference)

    // Label all of the vertices
    val labeledVertices = vertices.labelVertices(indexNames)

    val edgesWithCorrectedLabels = correctEdgeLabels(labeledVertices, edges)

    // Use Spark aggregation to figure out all of the Vertex and Edge schemas
    val vertexSchemaAggregator = new VertexSchemaAggregator(indexNames)
    val vertexSchemas = labeledVertices.aggregate(vertexSchemaAggregator.zeroValue)(vertexSchemaAggregator.seqOp, vertexSchemaAggregator.combOp).values
    val edgeSchemas = edgesWithCorrectedLabels.aggregate(EdgeSchemaAggregator.zeroValue)(EdgeSchemaAggregator.seqOp, EdgeSchemaAggregator.combOp).values

    // Create the target Graph
    val targetGraph = graphs.createGraph(GraphTemplate(None, "ia/frame"))

    // Create the Edge and Vertex frames
    vertexSchemas.foreach(schema => graphs.defineVertexType(targetGraph.toReference, schema))
    edgeSchemas.foreach(schema => graphs.defineEdgeType(targetGraph.toReference, schema))

    saveVertices(labeledVertices, targetGraph.toReference)
    saveEdges(edgesWithCorrectedLabels.map(_.edge), targetGraph.toReference)

    vertices.unpersist()
    edges.unpersist()

    graphs.updateIdCounter(targetGraph.toReference, Math.max(maxVertexId, maxEdgeId) + 1)

    graphs.expectGraph(targetGraph.toReference)
  }

  /**
   * Edges in "Seamless Graph" have a source vertex label and destination vertex label.
   * This is more restrictive than Titan so some edges from Titan might need to be re-labeled.
   *
   * For example, Titan may have 'ratings' edges that are directed from 'users' to 'movies' and
   * others that are directed from 'movies' to 'users'.  When these edges are put in Seamless graph
   * there will need to be two edge types, one per direction.
   *
   * @param labeledVertices vertices with an "_label" property
   * @param edges  edges with labels as they came out of Titan
   * @return edges with corrected labels (modified if needed)
   */
  def correctEdgeLabels(labeledVertices: RDD[GBVertex], edges: RDD[GBEdge]): RDD[EdgeHolder] = {

    // Join edges with vertex labels so that we can find the source and target for each edge type
    val vidsAndLabels = labeledVertices.map(vertex => (vertex.physicalId.asInstanceOf[Long], vertex.getProperty(GraphSchema.labelProperty).get.value.toString))
    val edgesByHead = edges.map(edge => (edge.headPhysicalId.asInstanceOf[Long], EdgeHolder(edge, null, null)))
    val edgesWithHeadLabels = edgesByHead.join(vidsAndLabels).values.map(pair => pair._1.copy(srcLabel = pair._2))
    val joined = edgesWithHeadLabels.map(e => (e.edge.tailPhysicalId.asInstanceOf[Long], e)).join(vidsAndLabels).values.map(pair => pair._1.copy(destLabel = pair._2))
    joined.cache()

    // Edges in "Seamless Graph" have a source vertex label and destination vertex label.
    // This is more restrictive than Titan so some edges from Titan might need to be re-labeled
    val labels = joined.map(e => LabelTriplet(e.edge.label, e.srcLabel, e.destLabel)).distinct().collect()
    val edgeLabelsMap = ExportToGraphPlugin.buildTargetMapping(labels)
    val edgesWithCorrectedLabels = joined.map(edgeHolder => {
      val targetLabel = edgeLabelsMap.get(LabelTriplet(edgeHolder.edge.label, edgeHolder.srcLabel, edgeHolder.destLabel))
      if (targetLabel.isEmpty) {
        throw new RuntimeException(s"targetLabel wasn't found in map $edgeLabelsMap")
      }
      EdgeHolder(edgeHolder.edge.copy(label = targetLabel.get), edgeHolder.srcLabel, edgeHolder.destLabel)
    })

    joined.unpersist()

    edgesWithCorrectedLabels
  }

  /**
   * Save the edges
   * @param edges edge rdd
   * @param targetGraph destination graph instance
   */
  def saveEdges(edges: RDD[GBEdge], targetGraph: GraphReference)(implicit invocation: Invocation) {
    graphs.expectSeamless(targetGraph.id).edgeFrames.foreach(edgeFrame => {
      val schema = edgeFrame.schema.asInstanceOf[EdgeSchema]
      val filteredEdges: RDD[GBEdge] = edges.filter(e => e.label == schema.label)

      val edgeWrapper = new EdgeWrapper(schema)
      val rows = filteredEdges.map(gbEdge => edgeWrapper.create(gbEdge))
      val edgeFrameRdd = new FrameRdd(schema, rows)
      frames.saveFrameData(edgeFrame.toReference, edgeFrameRdd)
    })
  }

  /**
   * Save the vertices
   * @param vertices vertices rdd
   * @param targetGraph destination graph instance
   */
  def saveVertices(vertices: RDD[GBVertex], targetGraph: GraphReference)(implicit invocation: Invocation) {
    graphs.expectSeamless(targetGraph.id).vertexFrames.foreach(vertexFrame => {
      val schema = vertexFrame.schema.asInstanceOf[VertexSchema]

      val filteredVertices: RDD[GBVertex] = vertices.filter(v => {
        v.getProperty(GraphSchema.labelProperty) match {
          case Some(p) => p.value == schema.label
          case _ => throw new RuntimeException(s"Vertex didn't have a label property $v")
        }
      })

      val vertexWrapper = new VertexWrapper(schema)
      val rows = filteredVertices.map(gbVertex => vertexWrapper.create(gbVertex))
      val vertexFrameRdd = new FrameRdd(schema, rows)
      frames.saveFrameData(vertexFrame.toReference, vertexFrameRdd)
    })
  }
}

object ExportToGraphPlugin {

  /**
   * Unique indices should exist on Vertex properties that were a user-defined Vertex ID
   * @return vertex properties that had 'unique' indices
   */
  def uniqueVertexPropertyIndices(graphs: SparkGraphStorage, titanGraph: GraphReference)(implicit context: Invocation): List[String] = {
    val graph = graphs.getTitanGraph(titanGraph)
    try {
      val indexes = graph.getManagementSystem.getGraphIndexes(classOf[Vertex])
      val uniqueIndexNames = indexes.iterator().toList.filter(_.isUnique).map(_.getName)
      uniqueIndexNames
    }
    finally {
      graph.shutdown()
    }
  }

  /**
   * Create mapping of Triplets to target labels.
   *
   * Edges in "Seamless Graph" have a source vertex label and destination vertex label.
   * This is more restrictive than Titan so some edges from Titan might need to be re-labeled
   *
   * @param labelTriplets labels as they exist in Titan
   * @return LabelTriplet to the target label that will actually be used during edge output
   */
  def buildTargetMapping(labelTriplets: Array[LabelTriplet]): Map[LabelTriplet, String] = {
    var usedLabels = Set[String]()

    var result = Map[LabelTriplet, String]()
    for (labelTriplet <- labelTriplets) {
      var label = labelTriplet.edgeLabel
      var counter = 1
      while (usedLabels.contains(label)) {
        label = labelTriplet + "_" + counter
        counter = counter + 1
      }
      usedLabels += label
      result += labelTriplet -> labelTriplet.edgeLabel
    }
    result
  }

}
