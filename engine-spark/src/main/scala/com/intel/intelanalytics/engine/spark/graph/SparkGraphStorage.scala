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
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.{ EntityRegistry, Rows, GraphBackendStorage, GraphStorage }
import com.intel.graphbuilder.driver.spark.titan.GraphBuilder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.repository.MetaStore
import scala.concurrent._
import ExecutionContext.Implicits.global
import com.intel.intelanalytics.domain.graph._
import com.intel.intelanalytics.engine.spark.frame.SparkFrameStorage
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.event.EventLogging

/**
 * Front end for Spark to create and manage graphs using GraphBuilder3
 * @param metaStore Repository for graph and frame meta data.
 * @param backendStorage Backend store the graph database.
 * @param frameStorage Provides dataframe services.
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

      //TODO: implement!
      ???
      //      val meta = getMetaData(reference)
      //      new SparkGraphData(meta.meta, storage.loadFrameData(sc, meta.meta))
    }

    override def getMetaData(reference: Reference): MetaData = new GraphMeta(expectGraph(reference.id))

    override def create()(implicit invocation: Invocation): Reference = storage.createGraph(GraphTemplate(generateGraphName()))

    override def getReference(id: Long): Reference = expectGraph(id)

    implicit def graphToRef(graph: Graph): Reference = GraphReference(graph.id, Some(true))

    implicit def sc(implicit invocation: Invocation): SparkContext = invocation.asInstanceOf[SparkInvocation].sparkContext

    implicit def user(implicit invocation: Invocation): UserPrincipal = invocation.user

  }

  EntityRegistry.register(GraphEntity, SparkGraphManagement)

  /**
   * Deletes a graph by synchronously deleting its information from the metastore and asynchronously
   * deleting it from the backendstorage.
   * @param graph Graph metadata object.
   */
  override def drop(graph: Graph): Unit = {

    metaStore.withSession("spark.graphstorage.drop") {
      implicit session =>
        {
          val quiet: Boolean = true
          backendStorage.deleteUnderlyingTable(graph.name, quiet)
          metaStore.graphRepo.delete(graph.id)
          Unit
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
          val quiet: Boolean = true
          backendStorage.deleteUnderlyingTable(graph.name, quiet)
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
          backendStorage.renameUnderlyingTable(graph.name, newName)

          val newGraph = graph.copy(name = newName)
          metaStore.graphRepo.update(newGraph).get
        }
    }
  }

  /**
   * Loads new data in an existing graph in the graph database. New data comes from a dataframe and is
   * interpreted as graph data by user specified rules.
   * @param graphLoad Command arguments, graph ID and source data frame iD
   * @param user The user loading the graph.
   * @return
   */
  override def loadGraph(graphLoad: GraphLoad, invocation: Invocation)(implicit user: UserPrincipal): Graph = {
    withContext("se.loadgraph") {
      metaStore.withSession("spark.graphstorage.load") {
        implicit session =>
          {
            val sparkContext = invocation.asInstanceOf[SparkInvocation].sparkContext

            val frameRules = graphLoad.frame_rules

            // TODO graphbuilder only supports one input frame at present
            require(frameRules.size == 1, "only one frame rule per call is supported in this version")

            val theOnlySourceFrameID = frameRules.head.frame.id

            val dataFrame = frameStorage.lookup(theOnlySourceFrameID)

            val graph = lookup(graphLoad.graph.id).get

            val gbConfigFactory = new GraphBuilderConfigFactory(dataFrame.get.schema, graphLoad, graph)

            val graphBuilder = new GraphBuilder(gbConfigFactory.graphConfig)

            // Setup data in Spark
            val inputRowsRdd: RDD[Rows.Row] = frameStorage.loadLegacyFrameRdd(sparkContext, theOnlySourceFrameID)

            val inputRdd: RDD[Seq[_]] = inputRowsRdd.map(x => x.toSeq)

            graphBuilder.build(inputRdd)

            graph
          }
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
   */
  override def lookup(id: Long): Option[Graph] = {
    metaStore.withSession("spark.graphstorage.lookup") {
      implicit session =>
        {
          metaStore.graphRepo.lookup(id)
        }
    }
  }

  /**
   * Get the metadata for a graph from its unique ID.
   * @param id the ID being looked up.
   */
  def expectGraph(id: Long): Graph = {
    lookup(id).getOrElse(throw new NotFoundException("graph", id.toString))
  }

  /**
   * Automatically generate a name for a graph.
   *
   * The frame name comprises of the prefix "graph_", a random uuid, and an optional annotation.
   *
   * @param annotation Optional annotation to add to graph name
   * @return Graph name
   */
  def generateGraphName(annotation: Option[String] = None): String = {
    "graph_" + java.util.UUID.randomUUID().toString + annotation.getOrElse("")
  }

}
