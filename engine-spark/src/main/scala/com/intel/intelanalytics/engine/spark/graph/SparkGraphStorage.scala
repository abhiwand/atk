package com.intel.intelanalytics.engine.spark.graph

import com.intel.intelanalytics.domain.{DataFrame, GraphTemplate, Graph}
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.{FrameComponent, Rows, GraphBackendStorage, GraphStorage}
import java.util.concurrent.atomic.AtomicLong
import com.intel.intelanalytics.engine.spark.graphbuilder.GraphBuilderConfigFactory
import com.intel.graphbuilder.driver.spark.titan.GraphBuilder
import org.apache.spark.rdd.RDD
import java.nio.file.Paths
import scala.io.{Codec, Source}
import spray.json.JsonParser
import scala.reflect.io.File
import com.intel.intelanalytics.engine.spark.{Context, SparkComponent}
import org.apache.spark.SparkContext


class SparkGraphStorage(context: (UserPrincipal) => Context, backendStorage: GraphBackendStorage,
                        frameStorage: SparkComponent#SparkFrameStorage)
  extends GraphStorage(backendStorage, frameStorage) {

    /*
    def lookup(id: Long): Option[Graph]
    def createGraph(graph: GraphTemplate)(implicit user: UserPrincipal): Graph
    def drop(graph: Graph)
 */




    val graphStorageBackend : GraphBackendStorage = new SparkGraphHBaseBackend {}

    //temporary
    var graphId = new AtomicLong(1)

    def nextGraphId() = {
      //Just a temporary implementation, only appropriate for scaffolding.
      graphId.getAndIncrement
    }

    import spray.json._

    import com.intel.intelanalytics.domain.DomainJsonProtocol._

    override def drop(graph: Graph): Unit = {

      println("DROPPING GRAPH: " + graph.name)
      backendStorage.deleteTable(graph.name)
      Unit
    }


    override def createGraph(graph: GraphTemplate)(implicit user: UserPrincipal): Graph = {

      println("CREATING GRAPH " + graph.graphName)

      val id = nextGraphId()
      val sparkContext = context(user).sparkContext

      val dataFrame = frameStorage.lookup(graph.dataFrameId)
      val gbConfigFactory = new GraphBuilderConfigFactory(dataFrame.get.schema, graph)

      val graphBuilder = new GraphBuilder(gbConfigFactory.graphConfig)

      // Setup data in Spark
      val inputRowsRdd: RDD[Rows.Row] = frameStorage.getFrameRdd(sparkContext, graph.dataFrameId)

      val inputRdd: RDD[Seq[_]] = inputRowsRdd.map(x => x.toSeq)

      graphBuilder.build(inputRdd)

      new Graph(id, graph.graphName)
    }


    override def getGraphs(offset: Int, count: Int)(implicit user: UserPrincipal) : Seq[Graph] = {
      println("LISTING " + count + " GRAPHS FROM " + offset)
      List[Graph]()
    }


    override def lookup(id: Long): Option[Graph] = withContext("graph.lookup") {
      val path = getFrameDirectory(id)
      val meta = File(Paths.get(path, "meta"))
      if (files.getMetaData(meta.path).isEmpty) {
        return None
      }
      val f = files.read(meta)
      try {
        val src = Source.fromInputStream(f)(Codec.UTF8).getLines().mkString("")
        val json = JsonParser(src)
        return Some(json.convertTo[DataFrame])
      }
      finally {
        f.close()
      }


}
