package com.intel.intelanalytics.engine.spark.graph

import com.intel.intelanalytics.domain.{GraphLoad, DataFrame, GraphTemplate, Graph}
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

class SparkGraphStorage(context: (UserPrincipal) => Context,
                        metaStore: MetaStoreComponent#MetaStore,
                        backendStorage: GraphBackendStorage,
                        frameStorage: SparkComponent#SparkFrameStorage)
    extends GraphStorage(backendStorage, frameStorage) {

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

  // override def createGraph(graph: GraphTemplate)(implicit user: UserPrincipal): Graph = {
  override def createGraph(graphLoad: GraphLoad[JsObject, Long])(implicit user: UserPrincipal): Future[Graph] = {
    metaStore.withSession("spark.graphstorage.createGraph") {
      implicit session =>
        {

          val sparkContext = context(user).sparkContext

          val dataFrame = frameStorage.lookup(graphLoad.sourceFrame)
          val gbConfigFactory = new GraphBuilderConfigFactory(dataFrame.get.schema, graphLoad)

          val graphBuilder = new GraphBuilder(gbConfigFactory.graphConfig)

          // Setup data in Spark
          val inputRowsRdd: RDD[Rows.Row] = frameStorage.getFrameRdd(sparkContext, graphLoad.dataFrameId)

          val inputRdd: RDD[Seq[_]] = inputRowsRdd.map(x => x.toSeq)

          graphBuilder.build(inputRdd)

          metaStore.graphRepo.insert(graphLoad).get
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
