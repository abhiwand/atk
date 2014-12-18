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
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.domain.schema._
import com.intel.intelanalytics.engine.spark.frame.{ LegacyFrameRDD, SparkFrameStorage }
import com.intel.intelanalytics.domain.schema.DataTypes._
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.graphbuilder.elements.Property
import com.thinkaurelius.titan.core.TitanGraph
import com.intel.intelanalytics.domain.graph.{ GraphMeta, Graph, GraphNoArgs, GraphTemplate }
import com.intel.intelanalytics.domain.schema.Column
import com.intel.intelanalytics.domain.schema.VertexSchema
import com.intel.graphbuilder.elements.{ GBVertex, GBEdge }
import org.apache.spark.SparkContext

// Implicits needed for JSON conversion

import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.SparkContext._

class ExportFromTitanPlugin(frames: SparkFrameStorage, graphs: SparkGraphStorage) extends SparkCommandPlugin[GraphNoArgs, Graph] {
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
  override def name: String = "graph:titan/export_from_titan"

  override def kryoRegistrator: Option[String] = None

  override def numberOfJobs(arguments: GraphNoArgs)(implicit invocation: Invocation): Int = {
    val graphMeta: GraphMeta = resolve(arguments.graph)
    val titanIAGraph = graphMeta.meta
    val labelToIdNameMapping: Map[String, String] = getVertexLabelToIdColumnMapping(titanIAGraph)
    val edgeDefinitions = getEdgeDefinitions(titanIAGraph)
    val numberOfVertexTypes = labelToIdNameMapping.size
    val numberOfEdgeTypes = edgeDefinitions.length
    val totalProgress = numberOfVertexTypes * 5 + numberOfEdgeTypes * 5 + 2
    totalProgress
  }

  /**
   * Plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */

  override def execute(arguments: GraphNoArgs)(implicit invocation: Invocation) = {

    val titanIAGraph: GraphMeta = resolve(arguments.graph)

    val (vertices, edges) = graphs.loadFromTitan(sc, titanIAGraph.meta)
    vertices.cache()

    val maxVertexId = vertices.map(v => v.physicalId.asInstanceOf[Long]).reduce((a, b) => Math.max(a, b))

    //record id column information for each vertex type
    val labelToIdNameMapping: Map[String, String] = getVertexLabelToIdColumnMapping(titanIAGraph.meta)
    val edgeDefinitions = getEdgeDefinitions(titanIAGraph.meta)

    val graph = graphs.createGraph(GraphTemplate(java.util.UUID.randomUUID.toString, "ia/frame"))

    ExportFromTitanPlugin.createVertexFrames(graphs, graph.id, labelToIdNameMapping.keySet.toList)
    val titanDBGraph = graphs.getTitanGraph(titanIAGraph.id)
    saveToVertexFrame(vertices, sc, labelToIdNameMapping, graph, titanDBGraph)
    vertices.unpersist()

    edges.cache()

    val maxEdgeId = edges.flatMap(e => e.eid).reduce((a, b) => Math.max(a, b))

    ExportFromTitanPlugin.createEdgeFrames(graphs, graph.id, edgeDefinitions)
    saveToEdgeFrame(edges, sc, graph, titanDBGraph)
    edges.unpersist()

    graphs.updateIdCounter(graph.id, Math.max(maxVertexId, maxEdgeId))
    graph
  }

  def getEdgeDefinitions(titanIAGraph: Graph): List[EdgeSchema] = {
    titanIAGraph.frameSchemaList.get.schemas.filter {
      case s: EdgeSchema => true
      case _ => false
    }.asInstanceOf[List[EdgeSchema]]
  }

  def getVertexLabelToIdColumnMapping(titanIAGraph: Graph): Map[String, String] = {
    val vertexSchemas = titanIAGraph.frameSchemaList.get.schemas.filter {
      case s: VertexSchema => true
      case _ => false
    }.asInstanceOf[List[VertexSchema]]

    vertexSchemas.map(v => v.label -> v.idColumnName.get).toMap
  }

  /**
   * Append edges to edge frames
   * @param edges edge rdd
   * @param ctx spark context
   * @param graph destination graph instance
   * @param titanDBGraph titan graph
   */
  def saveToEdgeFrame(edges: RDD[GBEdge], ctx: SparkContext, graph: Graph, titanDBGraph: TitanGraph)(implicit invocation: Invocation) {
    graphs.expectSeamless(graph.id).edgeFrames.foreach(edgeFrame => {
      val label = edgeFrame.schema.asInstanceOf[EdgeSchema].label
      val srcLabel = edgeFrame.schema.asInstanceOf[EdgeSchema].srcVertexLabel
      val destLabel = edgeFrame.schema.asInstanceOf[EdgeSchema].destVertexLabel
      val typeEdge: RDD[GBEdge] = edges.filter(e => e.label.equalsIgnoreCase(label))

      val firstEdge: GBEdge = typeEdge.take(1)(0)
      val columns = firstEdge.properties.map(p => p.key).toList
      val edgeRdd: RDD[Row] = typeEdge.map(gbEdge => {
        val srcVid = gbEdge.headPhysicalId
        val destVid = gbEdge.tailPhysicalId

        val properties = gbEdge.properties.map(p => {
          if (p.key == "_eid") {
            Property(p.key, gbEdge.eid.get)
          }
          else {
            p
          }
        })

        gbEdge.getPropertiesValueByColumns(columns, properties) ++ Array(srcVid, destVid, label)
      })

      val schema = new EdgeSchema(ExportFromTitanPlugin.getSchemaFromProperties(columns, titanDBGraph) ++ List(Column("_src_vid", int64), Column("_dest_vid", int64), Column("_label", string)), label, srcLabel, destLabel)
      val edgesToAdd = new LegacyFrameRDD(schema, edgeRdd).toFrameRDD()

      val existingEdgeData = graphs.loadEdgeRDD(ctx, edgeFrame.id)
      val combinedRdd = existingEdgeData.append(edgesToAdd)
      graphs.saveEdgeRdd(edgeFrame.id, combinedRdd)
    })
  }

  /**
   * Append vertices to vertex frames
   * @param vertices vertices rdd
   * @param ctx spark context
   * @param labelToIdNameMapping mapping between vertex label and id column. it is for getting id column information for specific vertex type
   * @param graph destination graph instance
   * @param titanDBGraph titan graph
   */
  def saveToVertexFrame(vertices: RDD[GBVertex], ctx: SparkContext,
                        labelToIdNameMapping: Map[String, String],
                        graph: Graph, titanDBGraph: TitanGraph)(implicit invocation: Invocation) {
    graphs.expectSeamless(graph.id).vertexFrames.foreach(vertexFrame => {
      val label = vertexFrame.schema.asInstanceOf[VertexSchema].label
      val typeVertex: RDD[GBVertex] = vertices.filter(v => {
        v.getProperty("_label") match {
          case Some(p) => p.value.toString.equalsIgnoreCase(label)
          case _ => false
        }
      })

      val firstVertex: GBVertex = typeVertex.take(1)(0)
      val columns = firstVertex.properties.map(p => p.key).toList

      val sourceRdd: RDD[Row] = typeVertex.map(gbVertex => {
        val properties = gbVertex.properties.map(p => {
          if (p.key == "_vid") {
            Property(p.key, gbVertex.physicalId)
          }
          else {
            p
          }
        })

        gbVertex.getPropertiesValueByColumns(columns, properties)
      })

      val idColumn = labelToIdNameMapping(label)
      val schema = new VertexSchema(ExportFromTitanPlugin.getSchemaFromProperties(columns, titanDBGraph), label, Some(idColumn))
      val source = new LegacyFrameRDD(schema, sourceRdd).toFrameRDD()
      val existingVertexData = graphs.loadVertexRDD(ctx, vertexFrame.id)
      val combinedRdd = existingVertexData.setIdColumnName(idColumn).append(source)
      graphs.saveVertexRDD(vertexFrame.id, combinedRdd)
    })
  }
}

object ExportFromTitanPlugin {

  /**
   * Create vertex frames for all the vertex types in the input vertex rdd
   * @param graphs graph storage
   * @param graphId destination graph id
   * @param vertexLabels vertex labels
   */
  def createVertexFrames(graphs: SparkGraphStorage, graphId: Long, vertexLabels: List[String])(implicit invocation: Invocation) {
    vertexLabels.foreach(label => {
      graphs.defineVertexType(graphId, VertexSchema(List(Column("_vid", int64), Column("_label", string)), label = label, idColumnName = None))
    })
  }

  /**
   * Create edge frames for all the edge types in the input edge rdd
   * @param graphs graph storage
   * @param graphId destination graph id
   * @param edgeDefinitions definitions for edge types
   */
  def createEdgeFrames(graphs: SparkGraphStorage, graphId: Long, edgeDefinitions: List[EdgeSchema])(implicit invocation: Invocation) {
    edgeDefinitions.foreach(edgeDef => {
      graphs.defineEdgeType(graphId, edgeDef)
    })
  }

  /**
   * Return a list of columns where each contains column name and data type.
   * @param columns column names
   * @param titanGraph titan graph
   * @return
   */
  def getSchemaFromProperties(columns: List[String], titanGraph: TitanGraph): List[Column] = {
    val manager = titanGraph.getManagementSystem
    val result = columns.map(c => {
      val dataType = javaTypeToIATType(manager.getPropertyKey(c).getDataType)
      Column(c, dataType)
    }).toList

    manager.commit()
    result
  }

  /**
   * Match java type object and return DataType instance.
   * @return DataType instance
   */
  def javaTypeToIATType = (a: java.lang.Class[_]) => {
    val intType = classOf[java.lang.Integer]
    val longType = classOf[java.lang.Long]
    val floatType = classOf[java.lang.Float]
    val doubleType = classOf[java.lang.Double]
    val stringType = classOf[java.lang.String]

    a match {
      case `intType` => int32
      case `longType` => int64
      case `floatType` => float32
      case `doubleType` => float64
      case `stringType` => string
      case _ => throw new IllegalArgumentException(s"unsupported type $a")
    }
  }
}

