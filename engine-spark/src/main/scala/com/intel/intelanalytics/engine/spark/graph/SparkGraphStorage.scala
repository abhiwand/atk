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

package com.intel.intelanalytics.engine.spark.graph

import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.NotFoundException
import com.intel.intelanalytics.domain._
import com.intel.graphbuilder.elements.{ GBVertex, GBEdge }
import com.intel.graphbuilder.elements.{ GraphElement, GBVertex, GBEdge }
import com.intel.intelanalytics.NotFoundException
import com.intel.intelanalytics.domain.frame.{ FrameReference, FrameName, FrameEntity }
import com.intel.intelanalytics.domain.schema.{ GraphSchema, EdgeSchema, VertexSchema }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.domain.schema.{ Schema, GraphSchema, EdgeSchema, VertexSchema }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.{ EntityTypeRegistry, Rows, GraphBackendStorage, GraphStorage }
import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
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
import com.intel.intelanalytics.engine.spark.frame.SparkFrameStorage
import com.intel.event.EventLogging
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.component.Boot
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.driver.spark.titan.reader.TitanReader
import com.thinkaurelius.titan.core.TitanGraph

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
  def updateLastReadDate(graph: GraphEntity): Option[GraphEntity] = {
    metaStore.withSession("graph.updateLastReadDate") {
      implicit session =>
        metaStore.graphRepo.updateLastReadDate(graph).toOption
    }
  }

  object SparkGraphManagement extends EntityManager[GraphEntityType.type] {

    override implicit val referenceTag = GraphEntityType.referenceTag

    override type Reference = GraphReference

    override type MetaData = GraphMeta

    override type Data = SparkGraphData

    override def getData(reference: Reference)(implicit invocation: Invocation): Data = {
      val meta = getMetaData(reference)
      new SparkGraphData(meta.meta, None)
    }

    override def getMetaData(reference: Reference)(implicit invocation: Invocation): MetaData = new GraphMeta(expectGraph(reference))

    override def create(args: CreateEntityArgs)(implicit invocation: Invocation): Reference = storage.createGraph(
      GraphTemplate(args.name))

    def getReference(reference: Reference)(implicit invocation: Invocation): Reference = expectGraph(reference)

    //
    // Do not use. Use getReference(reference: Reference) instead.
    //
    override def getReference(id: Long)(implicit invocation: Invocation): Reference = getReference(GraphReference(id))

    implicit def graphToRef(graph: GraphEntity): Reference = GraphReference(graph.id)

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
  EntityTypeRegistry.register(GraphEntityType, SparkGraphManagement)

  /** Lookup a Graph, throw an Exception if not found */
  override def expectGraph(graphRef: GraphReference)(implicit invocation: Invocation): GraphEntity = {
    metaStore.withSession("spark.graphstorage.lookup") {
      implicit session =>
        {
          metaStore.graphRepo.lookup(graphRef.graphId)
        }
    }.getOrElse(throw new NotFoundException("graph", graphRef.graphId.toString))
  }

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

  def expectSeamless(graphRef: GraphReference): SeamlessGraphMeta = expectSeamless(graphRef.id)

  /**
   * Deletes a graph by synchronously deleting its information from the meta-store and asynchronously
   * deleting it from the backend storage.
   * @param graph Graph metadata object.
   */
  override def drop(graph: GraphEntity)(implicit invocation: Invocation): Unit = {
    metaStore.withSession("spark.graphstorage.drop") {
      implicit session =>
        {
          info(s"dropping graph id:${graph.id}, name:${graph.name}, entityType:${graph.entityType}")
          if (graph.isTitan) {
            backendStorage.deleteUnderlyingTable(graph.storage, quiet = true)
          }
          metaStore.graphRepo.delete(graph.id).get
        }
    }
  }

  /**
   * @param graph the graph to be copied
   * @param name name to be given to the copied graph
   * @return
   */
  override def copyGraph(graph: GraphEntity, name: Option[String])(implicit invocation: Invocation): GraphEntity = {
    info(s"copying graph id:${graph.id}, name:${graph.name}, entityType:${graph.entityType}")

    val copiedEntity = if (graph.isTitan) {
      val graphCopy = createGraph(GraphTemplate(name, StorageFormats.HBaseTitan))
      backendStorage.copyUnderlyingTable(graph.storage, graphCopy.storage)
      graphCopy
    }
    else {
      val graphCopy = createGraph(GraphTemplate(name))
      val storageName = graphCopy.storage
      val graphMeta = expectSeamless(graph.toReference)
      val framesToCopy = graphMeta.frameEntities.map(_.toReference)
      val copiedFrames = frameStorage.copyFrames(framesToCopy, invocation.asInstanceOf[SparkInvocation].sparkContext)

      metaStore.withSession("spark.graphstorage.copyGraph") {
        implicit session =>
          {
            copiedFrames.foreach(frame => metaStore.frameRepo.update(frame.copy(graphId = Some(graphCopy.id), modifiedOn = new DateTime)))
            metaStore.graphRepo.update(graphCopy.copy(storage = storageName, storageFormat = "ia/frame")).get
          }
      }
    }
    // refresh from DB
    expectGraph(copiedEntity.toReference)
  }

  /**
   * Update status id of the graph
   * @param graph graph instance
   * @param newStatusId status id
   */
  override def updateStatus(graph: GraphEntity, newStatusId: Long): Unit = {
    metaStore.withSession("spark.graphstorage.rename") {
      implicit session =>
        {
          val newGraph = graph.copy(statusId = newStatusId)
          metaStore.graphRepo.update(newGraph).get
        }
    }
  }

  /**
   * Registers a new graph.
   * @param graph The graph being registered.
   * @return Graph metadata.
   */
  override def createGraph(graph: GraphTemplate)(implicit invocation: Invocation): GraphEntity = {
    metaStore.withSession("spark.graphstorage.create") {
      implicit session =>
        {
          val check = metaStore.graphRepo.lookupByName(graph.name)
          check match {
            case Some(g) => {
              if (g.statusId == Status.Active) {
                throw new RuntimeException("Graph with same name exists. Create aborted.")
              }
              else {
                metaStore.graphRepo.delete(g.id)
              }
            }
            case _ => //do nothing. it is fine that there is no existing graph with same name.
          }

          val graphEntity = metaStore.graphRepo.insert(graph).get
          val graphBackendName: String = if (graph.isTitan) {
            backendStorage.deleteUnderlyingTable(GraphBackendName.getGraphBackendName(graphEntity), quiet = true)
            GraphBackendName.getGraphBackendName(graphEntity)
          }
          else ""
          metaStore.graphRepo.update(graphEntity.copy(storage = graphBackendName)).get
        }
    }
  }

  override def renameGraph(graph: GraphEntity, newName: String)(implicit invocation: Invocation): GraphEntity = {
    metaStore.withSession("spark.graphstorage.rename") {
      implicit session =>
        {
          val check = metaStore.graphRepo.lookupByName(Some(newName))
          if (check.isDefined) {
            throw new RuntimeException("Graph with same name exists. Rename aborted.")
          }
          if (graph.isTitan) {
            backendStorage.renameUnderlyingTable(graph.storage, newName)
          }
          val newGraph = graph.copy(name = Some(newName))
          metaStore.graphRepo.update(newGraph).get
        }
    }
  }

  /**
   * Obtain the graph metadata for a range of graph IDs.
   * @return Sequence of graph metadata objects.
   */
  override def getGraphs()(implicit invocation: Invocation): Seq[GraphEntity] = {
    metaStore.withSession("spark.graphstorage.getGraphs") {
      implicit session =>
        {
          metaStore.graphRepo.scanAll().filter(g => g.statusId != Status.Deleted && g.statusId != Status.Dead && g.name.isDefined)
        }
    }
  }

  override def getGraphByName(name: Option[String])(implicit invocation: Invocation): Option[GraphEntity] = {
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
  @deprecated("please use expectGraph() instead")
  override def lookup(id: Long)(implicit invocation: Invocation): Option[GraphEntity] = {
    metaStore.withSession("spark.graphstorage.lookup") {
      implicit session =>
        {
          metaStore.graphRepo.lookup(id)
        }
    }
  }

  def updateIdCounter(graph: GraphReference, idCounter: Long)(implicit invocation: Invocation): Unit = {
    metaStore.withSession("spark.graphstorage.updateIdCounter") {
      implicit session =>
        {
          metaStore.graphRepo.updateIdCounter(graph.id, idCounter)
        }
    }
  }

  /**
   * Defining an Vertex creates an empty vertex list data frame.
   * @param graphRef unique id for graph meta data (already exists)
   * @param vertexSchema definition for this vertex type
   * @return the meta data for the graph
   */
  def defineVertexType(graphRef: GraphReference, vertexSchema: VertexSchema)(implicit invocation: Invocation): SeamlessGraphMeta = {
    val graph = expectSeamless(graphRef)
    val label = vertexSchema.label
    if (graph.isVertexOrEdgeLabel(label)) {
      throw new IllegalArgumentException(s"The label $label has already been defined in this graph")
    }
    metaStore.withSession("define.vertex") {
      implicit session =>
        {
          val frame = FrameEntity(0, None, vertexSchema, 1, new DateTime, new DateTime, graphId = Some(graphRef.id))
          metaStore.frameRepo.insert(frame)
        }
    }
    expectSeamless(graphRef)
  }

  /**
   * Defining an Edge creates an empty edge list data frame.
   * @param graphRef unique id for graph meta data (already exists)
   * @param edgeSchema definition for this edge type
   * @return the meta data for the graph
   */
  def defineEdgeType(graphRef: GraphReference, edgeSchema: EdgeSchema)(implicit invocation: Invocation): SeamlessGraphMeta = {
    val graph = expectSeamless(graphRef)
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
          val frame = FrameEntity(0, None, edgeSchema, 1, new DateTime,
            new DateTime, graphId = Some(graphRef.id))
          metaStore.frameRepo.insert(frame)
        }
    }
    expectSeamless(graphRef)
  }

  /**
   *
   * @param ctx
   * @param graphRef
   * @param vertexLabel
   * @return
   */
  def loadVertexRDD(ctx: SparkContext, graphRef: GraphReference, vertexLabel: String)(implicit invocation: Invocation): VertexFrameRDD = {
    val frame = expectSeamless(graphRef).vertexMeta(vertexLabel)
    val frameRdd = frameStorage.loadFrameData(ctx, frame)
    new VertexFrameRDD(frameRdd)
  }

  def loadVertexRDD(ctx: SparkContext, frameRef: FrameReference)(implicit invocation: Invocation): VertexFrameRDD = {
    val frameEntity = frameStorage.expectFrame(frameRef)
    require(frameEntity.isVertexFrame, "frame was not a vertex frame")
    val frameRdd = frameStorage.loadFrameData(ctx, frameEntity)
    new VertexFrameRDD(frameRdd)
  }

  def loadEdgeRDD(ctx: SparkContext, frameRef: FrameReference)(implicit invocation: Invocation): EdgeFrameRDD = {
    val frameEntity = frameStorage.expectFrame(frameRef)
    require(frameEntity.isEdgeFrame, "frame was not an edge frame")
    val frameRdd = frameStorage.loadFrameData(ctx, frameEntity)
    new EdgeFrameRDD(frameRdd)
  }

  // TODO: delete me if not needed?
  //  def loadEdgeFrameRDD(ctx: SparkContext, graphId: Long, edgeLabel: String): EdgeFrameRDD = {
  //    val frame = expectSeamless(graphId).edgeMeta(edgeLabel)
  //    val frameRdd = frames.loadFrameRDD(ctx, frame)
  //    new EdgeFrameRDD(frameRdd)
  //  }

  def loadGbVertices(ctx: SparkContext, graph: GraphEntity)(implicit invocation: Invocation): RDD[GBVertex] = {
    val graphEntity = expectGraph(graph.toReference)
    if (graphEntity.isSeamless) {
      val graphEntity = expectSeamless(graph.toReference)
      graphEntity.vertexFrames.map(frame => loadGbVerticesForFrame(ctx, frame.toReference)).reduce(_.union(_))
    }
    else {
      // load from Titan
      val titanReaderRDD: RDD[GraphElement] = getTitanReaderRDD(ctx, graph)
      import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
      val gbVertices: RDD[GBVertex] = titanReaderRDD.filterVertices()
      gbVertices
    }
  }

  def loadGbEdges(ctx: SparkContext, graph: GraphEntity)(implicit invocation: Invocation): RDD[GBEdge] = {
    val graphEntity = expectGraph(graph.toReference)
    if (graphEntity.isSeamless) {
      val graphMeta = expectSeamless(graph.toReference)
      graphMeta.edgeFrames.map(frame => loadGbEdgesForFrame(ctx, frame.toReference)).reduce(_.union(_))
    }
    else {
      // load from Titan
      val titanReaderRDD: RDD[GraphElement] = getTitanReaderRDD(ctx, graph)
      import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
      val gbEdges: RDD[GBEdge] = titanReaderRDD.filterEdges()
      gbEdges
    }
  }

  def loadGbElements(ctx: SparkContext, graph: GraphEntity)(implicit invocation: Invocation): (RDD[GBVertex], RDD[GBEdge]) = {
    val graphEntity = expectGraph(SparkGraphManagement.graphToRef(graph))

    if (graphEntity.isSeamless) {
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

  def getTitanReaderRDD(ctx: SparkContext, graph: GraphEntity): RDD[GraphElement] = {
    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph.name.get)
    val titanConnector = new TitanGraphConnector(titanConfig)

    // Read the graph from Titan
    val titanReader = new TitanReader(ctx, titanConnector)
    val titanReaderRDD = titanReader.read()
    titanReaderRDD
  }

  /**
   * Create a new Titan graph with the given name.
   * @param graphName Name of the graph to be written.
   * @param gbVertices RDD of vertices.
   * @param gbEdges RDD of edges
   * @param append if true will attempt to append to an existing graph
   */

  def writeToTitan(graph: GraphEntity,
                   gbVertices: RDD[GBVertex],
                   gbEdges: RDD[GBEdge],
                   append: Boolean = false)(implicit invocation: Invocation): Unit = {

    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph)
    val gb =
      new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append = append))

    gb.buildGraphWithSpark(gbVertices, gbEdges)
  }

  /**
   * Loads vertices and edges from Titan graph database
   * @param ctx Spark context
   * @param graph Graph metadata object
   * @return RDDs of vertices and edges
   */
  def loadFromTitan(ctx: SparkContext, graph: GraphEntity): (RDD[GBVertex], RDD[GBEdge]) = {
    val titanReaderRDD: RDD[GraphElement] = getTitanReaderRDD(ctx, graph)
    import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._

    //Cache data to prevent Titan reader from scanning HBase/Cassandra table twice to read vertices and edges
    titanReaderRDD.persist(StorageLevel.MEMORY_ONLY)
    val gbVertices: RDD[GBVertex] = titanReaderRDD.filterVertices()
    val gbEdges: RDD[GBEdge] = titanReaderRDD.filterEdges()
    titanReaderRDD.unpersist()

    (gbVertices, gbEdges)
  }

  def getTitanGraph(graphReference: GraphReference)(implicit invocation: Invocation): TitanGraph = {
    val graph = expectGraph(graphReference)

    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph.name.get)
    val titanConnector = new TitanGraphConnector(titanConfig)
    titanConnector.connect()
  }

  def loadGbVerticesForFrame(ctx: SparkContext, frameRef: FrameReference)(implicit invocation: Invocation): RDD[GBVertex] = {
    loadVertexRDD(ctx, frameRef).toGbVertexRDD
  }

  def loadGbEdgesForFrame(ctx: SparkContext, frameRef: FrameReference)(implicit invocation: Invocation): RDD[GBEdge] = {
    loadEdgeRDD(ctx, frameRef).toGbEdgeRDD
  }

  def saveVertexRDD(frameRef: FrameReference, vertexFrameRDD: VertexFrameRDD)(implicit invocation: Invocation) = {
    val frameEntity = frameStorage.expectFrame(frameRef)
    require(frameEntity.isVertexFrame, "frame was not a vertex frame")
    frameStorage.saveFrameData(frameEntity.toReference, vertexFrameRDD)
  }

  //  def saveVertexRDD(graphId: Long, vertexLabel: String, vertexFrameRdd: VertexFrameRDD, rowCount: Option[Long] = None) = {
  //    val frame = expectSeamless(graphId).vertexMeta(vertexLabel)
  //    frames.saveFrame(frame, vertexFrameRdd, rowCount)
  //  }

  //  def saveEdgeRDD(graphId: Long, edgeLabel: String, edgeFrameRdd: EdgeFrameRDD, rowCount: Option[Long] = None) = {
  //    val frame = expectSeamless(graphId).edgeMeta(edgeLabel)
  //    frames.saveFrame(frame, edgeFrameRdd, rowCount)
  //  }

  def saveEdgeRdd(frameRef: FrameReference, edgeFrameRDD: EdgeFrameRDD)(implicit invocation: Invocation) = {
    val frameEntity = frameStorage.expectFrame(frameRef)
    require(frameEntity.isEdgeFrame, "frame was not an edge frame")
    frameStorage.saveFrameData(frameEntity.toReference, edgeFrameRDD)
  }

}
