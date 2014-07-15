package com.intel.intelanalytics.engine.spark.graph.query

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.graph.GraphReference
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph
import com.tinkerpop.blueprints.Element
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import com.tinkerpop.pipes.util.structures.Row
import spray.json._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

case class QueryArgs(graph: GraphReference,
                     gremlin: String,
                     count: Option[Int],
                     offset: Option[Int])

case class QueryResult(results: Iterable[Any], runTimeSeconds: Double)

object GremlinQuery {

  val titanGraphs = scala.collection.mutable.Map[String, StandardTitanGraph]()

  def main(args: Array[String]): Unit = {

    import com.intel.intelanalytics.domain.DomainJsonProtocol._
    implicit val queryArgsFormat = jsonFormat4(QueryArgs)
    implicit val queryResultFormat = jsonFormat2(QueryResult)

    val cacheResetSize = 1500
    val engine = new GremlinGroovyScriptEngine(cacheResetSize)

    val tableName = "gog"
    val hBaseZookeeperQuorum = "10.10.68.157"
    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", "hbase")
    titanConfig.setProperty("storage.hostname", hBaseZookeeperQuorum)
    titanConfig.setProperty("storage.tablename", tableName)

    val titanConnector = new TitanGraphConnector(titanConfig)


    val graph = if (titanGraphs.contains(tableName)) {
      titanConnector.connect()
    }
    else {
      titanConnector.connect()
    }
    val bindings = engine.createBindings()
    bindings.put("g", graph)

    val obj = engine.eval("g.V", bindings)

    val it = obj match {
      case x: java.lang.Iterable[_] => x.toIterable
      case x => List(x).toIterable
    }

    val result = QueryResult(it, 1.5d)

    val json = it.map(parseGremlin(graph, _))
    println(json.toString)
  }

  def parseGremlin[T: JsonFormat : ClassTag](graph: StandardTitanGraph, obj: T) : JsValue= {
    import com.intel.intelanalytics.engine.spark.graph.query.GremlinJsonProtocol._
    implicit val gremlinFormat = new GremlinJsonFormat[T](graph)
    obj.toJson

  }
}

/*class GremlinQuery extends CommandPlugin[QueryArgs, QueryResult] {
  import DomainJsonProtocol._
  implicit val queryArgsFormat = jsonFormat4(QueryArgs)
  implicit val queryResultFormat = jsonFormat2(QueryResult)

  override def execute(invocation: Invocation, arguments: QueryArgs)(implicit user: UserPrincipal, executionContext: ExecutionContext): QueryResult = {
    val start = System.currentTimeMillis()
    val config: Config = configuration().get

    //    val graphFuture = invocation.engine.getGraph(arguments.graph.toLong)
    //    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    val cacheResetSize = 1500
    val engine = new GremlinGroovyScriptEngine(cacheResetSize)

    val tableName = "gog"
    val hBaseZookeeperQuorum = "10.10.68.157"

    val titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", "hbase")
    titanConfig.setProperty("storage.hostname", hBaseZookeeperQuorum)
    titanConfig.setProperty("storage.tablename", tableName)

    val titanConnector = new TitanGraphConnector(titanConfig)
    val graph = titanConnector.connect()
    val bindings = engine.createBindings()
    bindings.put("g", graph)

    val obj = engine.eval("g.V.name", bindings)
    val list = ListBuffer[String]()
    val results = obj match {
      case pipe: GremlinPipeline => {
        while (pipe.hasNext) {
          list += pipe.next().toString
        }
        list.toList
      }
      case _ => List("oh, oh!")
    }

    //val y = 3.toInt.toJson.asJsObject
    //val z = Seq(1, 2, 3).toJson //.asJsObject
    //val jsonObject = new JSONObject()
    // jsonObject.put("test", "xfsg")
    // val w = jsonObject.asInstanceOf[JsObject]
    //    val json = new JSONArray()
    //    val json = obj match {
    //      case pipe: Pipeline => {
    //
    //        while (pipe.hasNext) {
    //
    //        }
    //      }
    //    }
    val time = (System.currentTimeMillis() - start).toDouble / 1000.0
    QueryResult(results, time)

  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[QueryArgs]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: QueryResult): JsObject = returnValue.toJson.asJsObject
   */
/**
 * The name of the command, e.g. graphs/ml/loopy_belief_propagation
 */
/*
 override def name: String = "graphs/query/gremlin"

 //TODO: Replace with generic code that works on any case class
 override def serializeArguments(arguments: QueryArgs): JsObject = arguments.toJson.asJsObject()

}  */
