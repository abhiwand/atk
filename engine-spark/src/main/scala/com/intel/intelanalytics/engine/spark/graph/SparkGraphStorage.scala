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

import com.intel.intelanalytics.NotFoundException
import com.intel.intelanalytics.domain.EntityManager
import com.intel.graphbuilder.elements.{ GBVertex, GBEdge }
import com.intel.graphbuilder.elements.{ GraphElement, GBVertex, GBEdge }
import com.intel.intelanalytics.NotFoundException
import com.intel.intelanalytics.domain.frame.{ FrameName, DataFrame }
import com.intel.intelanalytics.domain.schema.{ GraphSchema, EdgeSchema, VertexSchema }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.domain.schema.{ Schema, GraphSchema, EdgeSchema, VertexSchema }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.{ EntityRegistry, Rows, GraphBackendStorage, GraphStorage }
import com.intel.graphbuilder.driver.spark.titan.GraphBuilder
import org.apache.spark.SparkContext
import com.intel.intelanalytics.engine.{ GraphBackendStorage, GraphStorage }
import org.apache.spark.SparkContext
import org.apache.spark.ia.graph.{ EdgeFrameRDD, VertexFrameRDD }
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.repository.MetaStore
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import scala.concurrent._
import ExecutionContext.Implicits.global
import com.intel.intelanalytics.domain.graph._
import com.intel.intelanalytics.domain.graph._
import com.intel.intelanalytics.engine.spark.frame.SparkFrameStorage
import com.intel.event.EventLogging
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.component.Boot
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.thinkaurelius.titan.core.TitanGraph
import com.intel.intelanalytics.domain.Naming

import scala.util.Try

/**
 * Front end for Spark to create and manage graphs using GraphBuilder3
 * @param metaStore Repository for graph and frame meta data.
 * @param backendStorage Backend store the graph database.
 */
class SparkGraphStorage(metaStore: MetaStore,
                        backendStorage: GraphBackendStorage,
                        frameStorage: SparkFrameStorage)
    extends GraphStorage with EventLogging { storage =>

  object SparkGraphManagement extends EntityManager[GraphEntity.type] {

    override implicit val referenceTag = GraphEntity.referenceTag

    override type Reference = GraphReference

    override type MetaData = GraphMeta

    override type Data = SparkGraphData

    override def getData(reference: Reference)(implicit invocation: Invocation): Data = {
      val meta = getMetaData(reference)
      new SparkGraphData(meta.meta, None)
    }

    override def getMetaData(reference: Reference)(implicit invocation: Invocation): MetaData = new GraphMeta(expectGraph(reference.id))

    override def create(annotation: Option[String] = None)(implicit invocation: Invocation): Reference = storage.createGraph(
      GraphTemplate(GraphName.validateOrGenerate(annotation)))

    override def getReference(id: Long)(implicit invocation: Invocation): Reference = expectGraph(id)

    implicit def graphToRef(graph: Graph): Reference = GraphReference(graph.id, Some(true))

    implicit def sc(implicit invocation: Invocation): SparkContext = invocation.asInstanceOf[SparkInvocation].sparkContext

    implicit def user(implicit invocation: Invocation): UserPrincipal = invocation.user

    //TODO: implement!
    /**
     * Save data of the given type, possibly creating a new object.
     */
    override def saveData(data: SparkGraphStorage.this.SparkGraphManagement.Data)(implicit invocation: Invocation): SparkGraphStorage.this.SparkGraphManagement.Data = ???

    /**
     * Creates an (empty) instance of the given type, reserving a URI
     */
    override def delete(reference: SparkGraphStorage.this.SparkGraphManagement.Reference)(implicit invocation: Invocation): Unit = {
      val meta = getMetaData(reference)
      drop(meta.meta)
    }
  }

  //TODO: enable
  //EntityRegistry.register(GraphEntity, SparkGraphManagement)
  //in the meantime,
  //Default resolver that simply creates a reference, with no guarantee that it is valid.
  EntityRegistry.register(GraphEntity, SparkGraphManagement)

  /** Lookup a Graph, throw an Exception if not found */
  override def expectGraph(graphId: Long)(implicit invocation: Invocation): Graph = {
    lookup(graphId).getOrElse(throw new NotFoundException("graph", graphId.toString))
  }

  /** Lookup a Graph, throw an Exception if not found */
  override def expectGraph(graphRef: GraphReference)(implicit invocation: Invocation): Graph = expectGraph(graphRef.id)

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
  override def drop(graph: Graph)(implicit invocation: Invocation): Unit = {
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
   * Update status id of the graph
   * @param graph graph instance
   * @param newStatusId status id
   */
  override def updateStatus(graph: Graph, newStatusId: StatusId.Value): Unit = {
    metaStore.withSession("spark.graphstorage.rename") {
      implicit session =>
        {
          val newGraph = graph.copy(statusId = newStatusId.id)
          metaStore.graphRepo.update(newGraph).get
        }
    }
  }

  /**
   * Registers a new graph.
   * @param graph The graph being registered.
   * @return Graph metadata.
   */
  override def createGraph(graph: GraphTemplate)(implicit invocation: Invocation): Graph = {
    metaStore.withSession("spark.graphstorage.create") {
      implicit session =>
        {
          val check = metaStore.graphRepo.lookupByName(graph.name)
          check match {
            case Some(g) => {
              if (g.statusId == StatusId.active.id) {
                throw new RuntimeException("Graph with same name exists. Create aborted.")
              }
              else {
                metaStore.graphRepo.delete(g.id)
              }
            }
            case _ => //do nothing. it is fine that there is no existing graph with same name.
          }
          backendStorage.deleteUnderlyingTable(graph.name, quiet = true)
          val g = Graph(1, graph.name, None, "", if (graph.storageFormat == "hbase/titan") StatusId.init.id else StatusId.active.id, graph.storageFormat, new DateTime(), new DateTime(), None, None)
          metaStore.graphRepo.insert(g).get
        }
    }
  }

  override def renameGraph(graph: Graph, newName: String)(implicit invocation: Invocation): Graph = {
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
   * @return Sequence of graph metadata objects.
   */
  override def getGraphs()(implicit invocation: Invocation): Seq[Graph] = {
    metaStore.withSession("spark.graphstorage.getGraphs") {
      implicit session =>
        {
          metaStore.graphRepo.scanAll()
        }
    }
  }

  override def getGraphByName(name: String)(implicit invocation: Invocation): Option[Graph] = {
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
   */
  override def lookup(id: Long)(implicit invocation: Invocation): Option[Graph] = {
    metaStore.withSession("spark.graphstorage.lookup") {
      implicit session =>
        {
          metaStore.graphRepo.lookup(id)
        }
    }
  }

  def updateIdCounter(id: Long, idCounter: Long)(implicit invocation: Invocation): Unit = {
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
  def defineVertexType(graphId: Long, vertexSchema: VertexSchema)(implicit invocation: Invocation): SeamlessGraphMeta = {
    val graph = expectSeamless(graphId)
    val label = vertexSchema.label
    if (graph.isVertexOrEdgeLabel(label)) {
      throw new IllegalArgumentException(s"The label $label has already been defined in this graph")
    }
    metaStore.withSession("define.vertex") {
      implicit session =>
        {
          val frame = DataFrame(0, Naming.generateName(prefix = Some("vertex_frame_")), vertexSchema, 1, new DateTime, new DateTime, graphId = Some(graphId))
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
  def defineEdgeType(graphId: Long, edgeSchema: EdgeSchema)(implicit invocation: Invocation): SeamlessGraphMeta = {
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
          val frame = DataFrame(0, Naming.generateName(prefix = Some("edge_frame_")), edgeSchema, 1, new DateTime,
            new DateTime, graphId = Some(graphId))
          metaStore.frameRepo.insert(frame)
        }
    }
    expectSeamless(graphId)
  }

  /**
   *
   * @param ctx
   * @param graphId
   * @param vertexLabel
   * @return
   */
  def loadVertexRDD(ctx: SparkContext, graphId: Long, vertexLabel: String)(implicit invocation: Invocation): VertexFrameRDD = {
    val frame = expectSeamless(graphId).vertexMeta(vertexLabel)
    val frameRdd = frameStorage.loadFrameData(ctx, frame)
    new VertexFrameRDD(frameRdd)
  }

  def loadVertexRDD(ctx: SparkContext, frameId: Long)(implicit invocation: Invocation): VertexFrameRDD = {
    val frameMeta = frameStorage.expectFrame(frameId)
    require(frameMeta.isVertexFrame, "frame was not a vertex frame")
    val frameRdd = frameStorage.loadFrameData(ctx, frameMeta)
    new VertexFrameRDD(frameRdd)
  }

  def loadEdgeRDD(ctx: SparkContext, frameId: Long)(implicit invocation: Invocation): EdgeFrameRDD = {
    val frameMeta = frameStorage.expectFrame(frameId)
    require(frameMeta.isEdgeFrame, "frame was not an edge frame")
    val frameRdd = frameStorage.loadFrameData(ctx, frameMeta)
    new EdgeFrameRDD(frameRdd)
  }

  // TODO: delete me if not needed?
  //  def loadEdgeFrameRDD(ctx: SparkContext, graphId: Long, edgeLabel: String): EdgeFrameRDD = {
  //    val frame = expectSeamless(graphId).edgeMeta(edgeLabel)
  //    val frameRdd = frames.loadFrameRDD(ctx, frame)
  //    new EdgeFrameRDD(frameRdd)
  //  }

  def loadGbVertices(ctx: SparkContext, graph: Graph)(implicit invocation: Invocation): RDD[GBVertex] = {
    val graphMeta = expectGraph(graph.id)
    if (graphMeta.isSeamless) {
      val graphMeta = expectSeamless(graph.id)
      graphMeta.vertexFrames.map(frame => loadGbVerticesForFrame(ctx, frame.id)).reduce(_.union(_))
    }
    else {
      // load from Titan
      val titanReaderRDD: RDD[GraphElement] = getTitanReaderRDD(ctx, graph)
      import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
      val gbVertices: RDD[GBVertex] = titanReaderRDD.filterVertices()
      gbVertices
    }
  }

  def loadGbEdges(ctx: SparkContext, graph: Graph)(implicit invocation: Invocation): RDD[GBEdge] = {
    val graphMeta = expectGraph(graph.id)
    if (graphMeta.isSeamless) {
      val graphMeta = expectSeamless(graph.id)
      graphMeta.edgeFrames.map(frame => loadGbEdgesForFrame(ctx, frame.id)).reduce(_.union(_))
    }
    else {
      // load from Titan
      val titanReaderRDD: RDD[GraphElement] = getTitanReaderRDD(ctx, graph)
      import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
      val gbEdges: RDD[GBEdge] = titanReaderRDD.filterEdges()
      gbEdges
    }
  }

  def loadGbElements(ctx: SparkContext, graph: Graph)(implicit invocation: Invocation): (RDD[GBVertex], RDD[GBEdge]) = {
    val graphMeta = expectGraph(graph.id)

    if (graphMeta.isSeamless) {
      val vertexRDD = loadGbVertices(ctx, graph)
      val edgeRDD = loadGbEdges(ctx, graph)
      (vertexRDD, edgeRDD)
    }
    else {
      //Prevents us from scanning the NoSQL table twice when loading vertices and edges
      //Scanning NoSQL tables is a very expensive operation.
      loadFromTitan(ctx, graph)
    }
  }

  def getTitanReaderRDD(ctx: SparkContext, graph: Graph): RDD[GraphElement] = {
    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph.name)
    val titanConnector = new TitanGraphConnector(titanConfig)

    // Read the graph from Titan
    val titanReader = new TitanReader(ctx, titanConnector)
    val titanReaderRDD = titanReader.read()
    titanReaderRDD
  }

  /**
   * Loads vertices and edges from Titan graph database
   * @param ctx Spark context
   * @param graph Graph metadata object
   * @return RDDs of vertices and edges
   */
  def loadFromTitan(ctx: SparkContext, graph: Graph): (RDD[GBVertex], RDD[GBEdge]) = {
    val titanReaderRDD: RDD[GraphElement] = getTitanReaderRDD(ctx, graph)
    import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._

    //Cache data to prevent Titan reader from scanning HBase/Cassandra table twice to read vertices and edges
    titanReaderRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val gbVertices: RDD[GBVertex] = titanReaderRDD.filterVertices()
    val gbEdges: RDD[GBEdge] = titanReaderRDD.filterEdges()
    titanReaderRDD.unpersist()

    (gbVertices, gbEdges)
  }

  def getTitanGraph(graphId: Long)(implicit invocation: Invocation): TitanGraph = {
    val graph = lookup(graphId).get

    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph.name)
    val titanConnector = new TitanGraphConnector(titanConfig)
    titanConnector.connect()
  }

  def loadGbVerticesForFrame(ctx: SparkContext, frameId: Long)(implicit invocation: Invocation): RDD[GBVertex] = {
    loadVertexRDD(ctx, frameId).toGbVertexRDD
  }

  def loadGbEdgesForFrame(ctx: SparkContext, frameId: Long)(implicit invocation: Invocation): RDD[GBEdge] = {
    loadEdgeRDD(ctx, frameId).toGbEdgeRDD
  }

  def saveVertexRDD(frameId: Long, vertexFrameRDD: VertexFrameRDD)(implicit invocation: Invocation) = {
    val frameMeta = frameStorage.expectFrame(frameId)
    require(frameMeta.isVertexFrame, "frame was not a vertex frame")
    frameStorage.saveFrameData(frameMeta.toReference, vertexFrameRDD)
  }

  //  def saveVertexRDD(graphId: Long, vertexLabel: String, vertexFrameRdd: VertexFrameRDD, rowCount: Option[Long] = None) = {
  //    val frame = expectSeamless(graphId).vertexMeta(vertexLabel)
  //    frames.saveFrame(frame, vertexFrameRdd, rowCount)
  //  }

  //  def saveEdgeRDD(graphId: Long, edgeLabel: String, edgeFrameRdd: EdgeFrameRDD, rowCount: Option[Long] = None) = {
  //    val frame = expectSeamless(graphId).edgeMeta(edgeLabel)
  //    frames.saveFrame(frame, edgeFrameRdd, rowCount)
  //  }

  def saveEdgeRdd(frameId: Long, edgeFrameRDD: EdgeFrameRDD)(implicit invocation: Invocation) = {
    val frameMeta = frameStorage.expectFrame(frameId)
    require(frameMeta.isEdgeFrame, "frame was not an edge frame")
    frameStorage.saveFrameData(frameMeta.toReference, edgeFrameRDD)
  }

  def updateFrameSchemaList(graphMeta: Graph, schemas: List[Schema])(implicit invocation: Invocation): Graph = {
    metaStore.withSession("spark.graphstorage.updateElementIDNames") {
      implicit session =>
        {
          val updatedGraph = graphMeta.copy(frameSchemaList = Some(new SchemaList(schemas)))
          metaStore.graphRepo.update(updatedGraph).get
        }
    }
  }

}
