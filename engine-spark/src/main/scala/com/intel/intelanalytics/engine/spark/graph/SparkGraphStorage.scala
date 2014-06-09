package com.intel.intelanalytics.engine.spark.graph

import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.{ Rows, GraphBackendStorage, GraphStorage }
import com.intel.graphbuilder.driver.spark.titan.GraphBuilder
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark.{ SparkComponent }
import com.intel.intelanalytics.repository.{MetaStore, MetaStoreComponent}
import com.intel.intelanalytics.shared.EventLogging
import scala.concurrent._
import ExecutionContext.Implicits.global
import com.intel.intelanalytics.domain.graph.{GraphLoad, GraphTemplate, Graph}
import com.intel.intelanalytics.engine.spark.context.Context
import com.intel.intelanalytics.engine.spark.frame.SparkFrameStorage

/**
 * Front end for Spark to create and manage graphs using GraphBuilder3
 * @param context User context.
 * @param metaStore Repository for graph and frame meta data.
 * @param backendStorage Backend store the graph database.
 * @param frameStorage Provides dataframe services.
 */
class SparkGraphStorage(context: (UserPrincipal) => Context,
                        metaStore: MetaStore,
                        backendStorage: GraphBackendStorage,
                        frameStorage: SparkFrameStorage)
    extends GraphStorage with EventLogging {

  import spray.json._

  /**
   * Deletes a graph by synchronously deleting its information from the metastore and asynchronously
   * deleting it from the backendstorage.
   * @param graph Graph metadata object.
   */
  override def drop(graph: Graph): Unit = {
    metaStore.withSession("spark.graphstorage.drop") {
      implicit session =>
        {
          future {
            backendStorage.deleteUnderlyingTable(graph.name)
          }

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
    metaStore.withSession("spark.graphstorage.drop") {
      implicit session =>
        {
          metaStore.graphRepo.insert(graph).get
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
  override def loadGraph(graphLoad: GraphLoad[JsObject, Long, Long])(implicit user: UserPrincipal): Graph = {
    withContext("se.loadgraph") {
      metaStore.withSession("spark.graphstorage.createGraph") {
        implicit session =>
          {
            val sparkContext = context(user).sparkContext

            val frameRules = graphLoad.frame_rules

            // TODO graphbuilder only supports one input frame at present
            require(frameRules.size == 1)
            val theOnlySourceFrameID = frameRules.head.frame

            val dataFrame = frameStorage.lookup(theOnlySourceFrameID)

            val graph = lookup(graphLoad.graph).get

            val gbConfigFactory = new GraphBuilderConfigFactory(dataFrame.get.schema, graphLoad, graph)

            val graphBuilder = new GraphBuilder(gbConfigFactory.graphConfig)

            // Setup data in Spark
            val inputRowsRdd: RDD[Rows.Row] = frameStorage.getFrameRdd(sparkContext, theOnlySourceFrameID)

            val inputRdd: RDD[Seq[_]] = inputRowsRdd.map(x => x.toSeq)

            graphBuilder.build(inputRdd)

            graph
          }
      }
    }
  }

  /**
   * Obtain the graph metadata for a range of graph IDs.
   * @param offset First graph to obtain.
   * @param count Number of graphs to obtain.
   * @param user The user listing the graphs.
   * @return Sequence of graph metadata objects.
   */
  override def getGraphs(offset: Int, count: Int)(implicit user: UserPrincipal): Seq[Graph] = {
    metaStore.withSession("spark.graphstorage.getGraphs") {
      implicit session =>
        {
          metaStore.graphRepo.scan(offset, count)
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
}
