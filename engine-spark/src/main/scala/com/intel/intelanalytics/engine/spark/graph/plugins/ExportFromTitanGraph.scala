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

import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.domain.schema._
import com.intel.intelanalytics.engine.spark.frame.{ LegacyFrameRDD, SparkFrameStorage }
import com.intel.intelanalytics.domain.schema.DataTypes._
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.graphbuilder.elements.Property
import com.thinkaurelius.titan.core.TitanGraph
import com.intel.intelanalytics.domain.graph.{ Graph, GraphNoArgs, GraphTemplate }
import scala.Some
import com.intel.intelanalytics.domain.schema.Column
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.domain.schema.Schema
import com.intel.intelanalytics.domain.schema.VertexSchema
import com.intel.graphbuilder.elements.{ GBVertex, GBEdge }
import org.apache.spark.SparkContext

// Implicits needed for JSON conversion

import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.SparkContext._

class ExportFromTitanGraph(frames: SparkFrameStorage, graphs: SparkGraphStorage) extends SparkCommandPlugin[GraphNoArgs, Graph] {
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

  override def numberOfJobs(arguments: GraphNoArgs): Int = {
    val numberOfVertexTypes = 2
    val numberOfEdgeTypes = 1
    val totalProgress = numberOfVertexTypes * 5 + numberOfEdgeTypes * 5 + 3
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

  override def execute(invocation: SparkInvocation, arguments: GraphNoArgs)(implicit user: UserPrincipal, executionContext: ExecutionContext): Graph = {
    val ctx = invocation.sparkContext
    val graphId: Long = arguments.graph.id

    val titanIAGraph = graphs.expectGraph(graphId)

    val (vertices, edges) = graphs.loadFromTitan(ctx, titanIAGraph)
    vertices.cache()

    val maxVertexId = vertices.map(v => v.physicalId.asInstanceOf[Long]).reduce((a, b) => Math.max(a, b))

    //record id column information for each vertex type
    val labelToIdNameMapping: Map[String, String] = titanIAGraph.frameSchemaList.get.schemas.filter(schema => schema.vertexSchema != None).map(e => e.label.get -> e.vertexSchema.get.idColumnName.get).toMap
    val graph = graphs.createGraph(GraphTemplate(java.util.UUID.randomUUID.toString, "ia/frame"))

    ExportFromTitanGraph.createVertexFrames(graphs, graph.id, vertices, labelToIdNameMapping.keySet.toList)
    val titanDBGraph = graphs.getTitanGraph(graphId)
    saveToVertexFrame(vertices, ctx, labelToIdNameMapping, graph, titanDBGraph)
    vertices.unpersist()

    edges.cache()

    val maxEdgeId = edges.flatMap(e => e.eid).reduce((a, b) => Math.max(a, b))

    ExportFromTitanGraph.createEdgeFrames(edges, titanDBGraph, graphs, graph.id)
    saveToEdgeFrame(edges, ctx, graph, titanDBGraph)
    edges.unpersist()

    graphs.updateIdCounter(graph.id, Math.max(maxVertexId, maxEdgeId))
    graph
  }

  /**
   * Append edges to edge frames
   * @param edges edge rdd
   * @param ctx spark context
   * @param graph destination graph instance
   * @param titanDBGraph titan graph
   */
  def saveToEdgeFrame(edges: RDD[GBEdge], ctx: SparkContext, graph: Graph, titanDBGraph: TitanGraph) {
    graphs.expectSeamless(graph.id).edgeFrames.foreach(edgeFrame => {
      val label = edgeFrame.schema.edgeSchema.get.label
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

        gbEdge.getPropertiesValueByColumns(columns, properties) ++ Array(srcVid, destVid)
      })

      val schema = new Schema(ExportFromTitanGraph.getSchemaFromProperties(columns, titanDBGraph) ++ List(Column("_src_vid", int64), Column("_dest_vid", int64)))
      val edgesToAdd = new LegacyFrameRDD(schema, edgeRdd).toFrameRDD()

      val existingEdgeData = graphs.loadEdgeRDD(ctx, edgeFrame.id)
      val combinedRdd = existingEdgeData.append(edgesToAdd)

      combinedRdd.cache()
      graphs.saveEdgeRdd(edgeFrame.id, combinedRdd, Some(combinedRdd.count()))
      combinedRdd.unpersist()
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
  def saveToVertexFrame(vertices: RDD[GBVertex], ctx: SparkContext, labelToIdNameMapping: Map[String, String], graph: Graph, titanDBGraph: TitanGraph) {
    graphs.expectSeamless(graph.id).vertexFrames.foreach(vertexFrame => {
      val label = vertexFrame.schema.vertexSchema.get.label
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
      val schema = new Schema(ExportFromTitanGraph.getSchemaFromProperties(columns, titanDBGraph))
      val source = new LegacyFrameRDD(schema, sourceRdd).toFrameRDD()
      val existingVertexData = graphs.loadVertexRDD(ctx, vertexFrame.id)
      val combinedRdd = existingVertexData.setIdColumnName(idColumn).append(source)
      graphs.saveVertexRDD(vertexFrame.id, combinedRdd, Some(combinedRdd.count()))
    })
  }
}

object ExportFromTitanGraph {

  /**
   * Create vertex frames for all the vertex types in the input vertex rdd
   * @param graphs graph storage
   * @param graphId destination graph id
   * @param vertices vertices rdd
   */
  def createVertexFrames(graphs: SparkGraphStorage, graphId: Long, vertices: RDD[GBVertex], vertexLabels: List[String]) {
    vertexLabels.foreach(label => {
      graphs.defineVertexType(graphId, VertexSchema(label, None))
    })
  }

  /**
   * Create edge frames for all the edge types in the input edge rdd
   * @param edges edge rdd
   * @param titanDBGraph titan graph
   * @param graphs graph storage
   * @param graphId destination graph id
   */
  def createEdgeFrames(edges: RDD[GBEdge], titanDBGraph: TitanGraph, graphs: SparkGraphStorage, graphId: Long) {
    val edgeSampleByLabel = edges.map(e => (e.label, e)).reduceByKey((a, b) => a).collect()

    val edgeDefinitions = edgeSampleByLabel.map {
      case (label, edge) => {
        val srcId = edge.headPhysicalId
        val destId = edge.tailPhysicalId

        val srcVertex = titanDBGraph.getVertex(srcId)
        val destVertex = titanDBGraph.getVertex(destId)

        val srcLabel = srcVertex.getProperty("_label").asInstanceOf[String]
        val destLabel = destVertex.getProperty("_label").asInstanceOf[String]

        EdgeSchema(label, srcLabel, destLabel)
      }
    }

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

