package com.intel.intelanalytics.engine.spark.graph

import com.intel.intelanalytics.domain.{ GraphLoad, DataFrame, GraphTemplate, Graph }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.{ FrameComponent, Rows, GraphBackendStorage, GraphStorage }
import java.util.concurrent.atomic.AtomicLong
import com.intel.intelanalytics.engine.spark.graphbuilder.GraphBuilderConfigFactory
import com.intel.graphbuilder.driver.spark.titan.GraphBuilder
import org.apache.spark.rdd.RDD
import java.nio.file.Paths
import scala.io.{ Codec, Source }
import spray.json.JsonParser
import scala.reflect.io.File
import com.intel.intelanalytics.engine.spark.{ Context, SparkComponent }
import org.apache.spark.SparkContext
import com.intel.intelanalytics.repository.{ MetaStoreComponent, Repository }
import scala.concurrent.Future
import com.intel.intelanalytics.shared.EventLogging

class SparkGraphStorage(context: (UserPrincipal) => Context,
                        metaStore: MetaStoreComponent#MetaStore,
                        backendStorage: GraphBackendStorage,
                        frameStorage: SparkComponent#SparkFrameStorage)
    extends GraphStorage(backendStorage, frameStorage) with EventLogging {

  val graphStorageBackend: GraphBackendStorage = new SparkGraphHBaseBackend {}

  import spray.json._

  import com.intel.intelanalytics.domain.DomainJsonProtocol._

  override def drop(graph: Graph): Unit = {
    metaStore.withSession("spark.graphstorage.drop") {
      implicit session =>
        {

          backendStorage.deleteTable(graph.name)
          metaStore.graphRepo.delete(graph.id)

          Unit
        }
    }
  }

  override def createGraph(graph: GraphTemplate)(implicit user: UserPrincipal): Graph = {
    metaStore.withSession("spark.graphstorage.drop") {
      implicit session =>
        {
          metaStore.graphRepo.insert(graph).get
        }
    }
  }

  override def loadGraph(graphLoad: GraphLoad[JsObject, Long, Long])(implicit user: UserPrincipal): Graph = {
    withContext("se.runAls") {
      metaStore.withSession("spark.graphstorage.createGraph") {
        implicit session =>
          {
            val sparkContext = context(user).sparkContext

            val sourceFrameID = graphLoad.sourceFrameURI

            val dataFrame = frameStorage.lookup(sourceFrameID)

            val graph = lookup(graphLoad.graphURI).get

            val gbConfigFactory = new GraphBuilderConfigFactory(dataFrame.get.schema, graphLoad, graph)

            val graphBuilder = new GraphBuilder(gbConfigFactory.graphConfig)

            // Setup data in Spark
            val inputRowsRdd: RDD[Rows.Row] = frameStorage.getFrameRdd(sparkContext, sourceFrameID)

            val inputRdd: RDD[Seq[_]] = inputRowsRdd.map(x => x.toSeq)

            graphBuilder.build(inputRdd)

            graph
          }
      }
    }
  }

  override def getGraphs(offset: Int, count: Int)(implicit user: UserPrincipal): Seq[Graph] = {
    metaStore.withSession("spark.graphstorage.getGraphs") {
      implicit session =>
        {
          metaStore.graphRepo.scan(offset, count)
        }
    }
  }

  override def lookup(id: Long): Option[Graph] = {
    metaStore.withSession("spark.graphstorage.lookup") {
      implicit session =>
        {
          metaStore.graphRepo.lookup(id)
        }
    }
  }
}
