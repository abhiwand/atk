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

package com.intel.intelanalytics.engine.spark.graph

import com.intel.graphbuilder.elements.{ GBVertex, GBEdge }
import com.intel.intelanalytics.NotFoundException
import com.intel.intelanalytics.domain.frame.{ FrameName, DataFrame }
import com.intel.intelanalytics.domain.schema.{ GraphSchema, EdgeSchema, VertexSchema }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.{ GraphBackendStorage, GraphStorage }
import org.apache.spark.SparkContext
import org.apache.spark.ia.graph.{ EdgeFrameRDD, VertexFrameRDD }
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.repository.MetaStore
import org.joda.time.DateTime
import scala.concurrent._
import com.intel.intelanalytics.domain.graph._
import com.intel.intelanalytics.engine.spark.frame.SparkFrameStorage
import com.intel.event.EventLogging
import com.intel.intelanalytics.domain.Naming

/**
 * Front end for Spark to create and manage graphs using GraphBuilder3
 * @param metaStore Repository for graph and frame meta data.
 * @param backendStorage Backend store the graph database.
 * @param frames Provides dataframe services.
 */
class SparkGraphStorage(metaStore: MetaStore,
                        backendStorage: GraphBackendStorage,
                        frames: SparkFrameStorage)
    extends GraphStorage with EventLogging {

  /** Lookup a Graph, throw an Exception if not found */
  override def expectGraph(graphId: Long): Graph = {
    lookup(graphId).getOrElse(throw new NotFoundException("graph", graphId.toString))
  }

  /** Lookup a Graph, throw an Exception if not found */
  override def expectGraph(graphRef: GraphReference): Graph = expectGraph(graphRef.id)

  /**
   * Lookup a Seamless Graph, throw an Exception if not found
   *
   * "Seamless Graph" is a graph that provides a "seamless user experience" between graphs and frames.
   * The same data can be treated as frames one moment and as a graph the next without any import/export.
   */
  def expectSeamless(graphId: Long): SeamlessGraphMeta = {
    metaStore.withSession("seamless.graph.storage") {
      implicit session =>
        {
          val graph = metaStore.graphRepo.lookup(graphId).getOrElse(throw new NotFoundException("graph", graphId.toString))
          require(graph.isSeamless, "graph existed but did not have the expected storage format")
          val frames = metaStore.frameRepo.lookupByGraphId(graphId)
          SeamlessGraphMeta(graph, frames.toList)
        }
    }
  }

  /**
   * Deletes a graph by synchronously deleting its information from the meta-store and asynchronously
   * deleting it from the backend storage.
   * @param graph Graph metadata object.
   */
  override def drop(graph: Graph): Unit = {
    metaStore.withSession("spark.graphstorage.drop") {
      implicit session =>
        {
          if (graph.isTitan) {
            backendStorage.deleteUnderlyingTable(graph.name, quiet = true)
          }
          metaStore.graphRepo.delete(graph.id)
        }
    }
  }

  /**
   * Registers a new graph.
   * @param graph The graph being registered.
   * @param user The user creating the graph.
   * @return Graph metadata.
   */
  override def createGraph(graph: GraphTemplate)(implicit user: UserPrincipal): Graph = {

    metaStore.withSession("spark.graphstorage.create") {
      implicit session =>
        {
          val check = metaStore.graphRepo.lookupByName(graph.name)
          if (check.isDefined) {
            throw new RuntimeException("Graph with same name exists. Create aborted.")
          }
          backendStorage.deleteUnderlyingTable(graph.name, quiet = true)
          metaStore.graphRepo.insert(graph).get
        }
    }
  }

  override def renameGraph(graph: Graph, newName: String): Graph = {
    metaStore.withSession("spark.graphstorage.rename") {
      implicit session =>
        {
          val check = metaStore.graphRepo.lookupByName(newName)
          if (check.isDefined) {
            throw new RuntimeException("Graph with same name exists. Rename aborted.")
          }
          if (graph.isTitan) {
            backendStorage.renameUnderlyingTable(graph.name, newName)
          }
          val newGraph = graph.copy(name = newName)
          metaStore.graphRepo.update(newGraph).get
        }
    }
  }

  /**
   * Obtain the graph metadata for a range of graph IDs.
   * @param user The user listing the graphs.
   * @return Sequence of graph metadata objects.
   */
  override def getGraphs()(implicit user: UserPrincipal): Seq[Graph] = {
    metaStore.withSession("spark.graphstorage.getGraphs") {
      implicit session =>
        {
          metaStore.graphRepo.scanAll()
        }
    }
  }

  override def getGraphByName(name: String)(implicit user: UserPrincipal): Option[Graph] = {
    metaStore.withSession("spark.graphstorage.getGraphByName") {
      implicit session =>
        {
          metaStore.graphRepo.lookupByName(name)
        }
    }
  }
  /**
   * Get the metadata for a graph from its unique ID.
   * @param id ID being looked up.
   * @return Future of Graph metadata.
   */
  override def lookup(id: Long): Option[Graph] = {
    metaStore.withSession("spark.graphstorage.lookup") {
      implicit session =>
        {
          metaStore.graphRepo.lookup(id)
        }
    }
  }

  def updateIdCounter(id: Long, idCounter: Long): Unit = {
    metaStore.withSession("spark.graphstorage.updateIdCounter") {
      implicit session =>
        {
          metaStore.graphRepo.updateIdCounter(id, idCounter)
        }
    }
  }

  /**
   * Defining an Vertex creates an empty vertex list data frame.
   * @param graphId unique id for graph meta data (already exists)
   * @param vertexSchema definition for this vertex type
   * @return the meta data for the graph
   */
  def defineVertexType(graphId: Long, vertexSchema: VertexSchema): SeamlessGraphMeta = {
    val graph = expectSeamless(graphId)
    val label = vertexSchema.label
    if (graph.isVertexOrEdgeLabel(label)) {
      throw new IllegalArgumentException(s"The label $label has already been defined in this graph")
    }
    metaStore.withSession("define.vertex") {
      implicit session =>
        {
          val schema = GraphSchema.defineVertexType(vertexSchema)
          val frame = DataFrame(0, Naming.generateName(prefix = Some("vertex_frame_")), None, schema, 0, 1, new DateTime, new DateTime, graphId = Some(graphId))
          metaStore.frameRepo.insert(frame)
        }
    }
    expectSeamless(graphId)
  }

  /**
   * Defining an Edge creates an empty edge list data frame.
   * @param graphId unique id for graph meta data (already exists)
   * @param edgeSchema definition for this edge type
   * @return the meta data for the graph
   */
  def defineEdgeType(graphId: Long, edgeSchema: EdgeSchema): SeamlessGraphMeta = {
    val graph = expectSeamless(graphId)
    if (graph.isVertexOrEdgeLabel(edgeSchema.label)) {
      throw new IllegalArgumentException(s"The label ${edgeSchema.label} has already been defined in this graph")
    }
    else if (!graph.isVertexLabel(edgeSchema.srcVertexLabel)) {
      throw new IllegalArgumentException(s"source vertex type ${edgeSchema.srcVertexLabel} is not defined in this graph")
    }
    else if (!graph.isVertexLabel(edgeSchema.destVertexLabel)) {
      throw new IllegalArgumentException(s"destination vertex type ${edgeSchema.destVertexLabel} is not defined in this graph")
    }

    metaStore.withSession("define.vertex") {
      implicit session =>
        {
          val schema = GraphSchema.defineEdgeType(edgeSchema)
          val frame = DataFrame(0, Naming.generateName(prefix = Some("edge_frame_")), None, schema, 0, 1, new DateTime, new DateTime, graphId = Some(graphId))
          metaStore.frameRepo.insert(frame)
        }
    }
    expectSeamless(graphId)
  }

  def loadVertexRDD(ctx: SparkContext, graphId: Long, vertexLabel: String): VertexFrameRDD = {
    val frame = expectSeamless(graphId).vertexMeta(vertexLabel)
    val frameRdd = frames.loadFrameRDD(ctx, frame)
    new VertexFrameRDD(frameRdd)
  }

  def loadVertexRDD(ctx: SparkContext, frameId: Long): VertexFrameRDD = {
    val frameMeta = frames.expectFrame(frameId)
    require(frameMeta.isVertexFrame, "frame was not a vertex frame")
    val frameRdd = frames.loadFrameRDD(ctx, frameMeta)
    new VertexFrameRDD(frameRdd)
  }

  def loadEdgeRDD(ctx: SparkContext, frameId: Long): EdgeFrameRDD = {
    val frameMeta = frames.expectFrame(frameId)
    require(frameMeta.isEdgeFrame, "frame was not an edge frame")
    val frameRdd = frames.loadFrameRDD(ctx, frameMeta)
    new EdgeFrameRDD(frameRdd)
  }

  // TODO: delete me if not needed?
  //  def loadEdgeFrameRDD(ctx: SparkContext, graphId: Long, edgeLabel: String): EdgeFrameRDD = {
  //    val frame = expectSeamless(graphId).edgeMeta(edgeLabel)
  //    val frameRdd = frames.loadFrameRDD(ctx, frame)
  //    new EdgeFrameRDD(frameRdd)
  //  }

  def loadGbVertices(ctx: SparkContext, graphId: Long): RDD[GBVertex] = {
    val graphMeta = expectGraph(graphId)
    if (graphMeta.isSeamless) {
      val graphMeta = expectSeamless(graphId)
      graphMeta.vertexFrames.map(frame => loadGbVerticesForFrame(ctx, frame.id)).reduce(_.union(_))
    }
    else {
      // load from Titan
      ???
    }
  }

  def loadGbEdges(ctx: SparkContext, graphId: Long): RDD[GBEdge] = {
    val graphMeta = expectGraph(graphId)
    if (graphMeta.isSeamless) {
      val graphMeta = expectSeamless(graphId)
      graphMeta.edgeFrames.map(frame => loadGbEdgesForFrame(ctx, frame.id)).reduce(_.union(_))
    }
    else {
      // load from Titan
      ???
    }
  }

  def loadGbVerticesForFrame(ctx: SparkContext, frameId: Long): RDD[GBVertex] = {
    loadVertexRDD(ctx, frameId).toGbVertexRDD
  }

  def loadGbEdgesForFrame(ctx: SparkContext, frameId: Long): RDD[GBEdge] = {
    loadEdgeRDD(ctx, frameId).toGbEdgeRDD
  }

  def saveVertexRDD(frameId: Long, vertexFrameRDD: VertexFrameRDD, rowCount: Option[Long] = None) = {
    val frameMeta = frames.expectFrame(frameId)
    require(frameMeta.isVertexFrame, "frame was not a vertex frame")
    frames.saveFrame(frameMeta, vertexFrameRDD, rowCount)
  }

  //  def saveVertexRDD(graphId: Long, vertexLabel: String, vertexFrameRdd: VertexFrameRDD, rowCount: Option[Long] = None) = {
  //    val frame = expectSeamless(graphId).vertexMeta(vertexLabel)
  //    frames.saveFrame(frame, vertexFrameRdd, rowCount)
  //  }

  //  def saveEdgeRDD(graphId: Long, edgeLabel: String, edgeFrameRdd: EdgeFrameRDD, rowCount: Option[Long] = None) = {
  //    val frame = expectSeamless(graphId).edgeMeta(edgeLabel)
  //    frames.saveFrame(frame, edgeFrameRdd, rowCount)
  //  }

  def saveEdgeRdd(frameId: Long, edgeFrameRDD: EdgeFrameRDD, rowCount: Option[Long] = None) = {
    val frameMeta = frames.expectFrame(frameId)
    require(frameMeta.isEdgeFrame, "frame was not an edge frame")
    frames.saveFrame(frameMeta, edgeFrameRDD, rowCount)
  }

  def updateElementIDNames(graphMeta: Graph, elementIDColumns: List[ElementIDName]): Graph = {
    metaStore.withSession("spark.graphstorage.updateElementIDNames") {
      implicit session =>
        {
          val updatedGraph = graphMeta.copy(elementIDNames = Some(new ElementIDNames(elementIDColumns)))
          metaStore.graphRepo.update(updatedGraph).get
        }
    }
  }

}
