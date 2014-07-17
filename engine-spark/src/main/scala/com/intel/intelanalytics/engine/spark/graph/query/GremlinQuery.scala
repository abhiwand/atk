package com.intel.intelanalytics.engine.spark.graph.query

import javax.script.Bindings

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.{ Await, ExecutionContext, Lock }

/**
 *
 * @param graph
 * @param gremlin
 */
case class QueryArgs(graph: GraphReference, gremlin: String)

/**
 *
 * @param results
 * @param runTimeSeconds
 */
case class QueryResult(results: Iterable[JsValue], runTimeSeconds: Double)

/**
 *
 */
class GremlinQuery extends CommandPlugin[QueryArgs, QueryResult] {

  import com.intel.intelanalytics.domain.DomainJsonProtocol._

  implicit val queryArgsFormat = jsonFormat2(QueryArgs)
  implicit val queryResultFormat = jsonFormat2(QueryResult)

  val gremlinExecutor = new GremlinGroovyScriptEngine()
  var titanGraphs = Map[String, TitanGraph]()

  /**
   * Execute gremlin query.
   *
   * @param gremlinScript Gremlin query
   * @param bindings Bindings for Gremlin engine
   * @return Iterable of query results
   */
  def executeGremlinQuery(gremlinScript: String, bindings: Bindings): Iterable[Any] = {
    val obj = gremlinExecutor.eval(gremlinScript, bindings)
    if (obj == null) throw new RuntimeException(s"Unable to execute Gremlin query: ${gremlinScript}")

    val resultIterator = obj match {
      case x: java.lang.Iterable[_] => x.toIterable
      case x => List(x).toIterable
    }
    resultIterator
  }

  /**
   *
   * @param invocation information about the user and the circumstances at the time of the call
   * @param arguments the arguments supplied by the caller
   * @param user
   * @param executionContext
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: Invocation, arguments: QueryArgs)(implicit user: UserPrincipal, executionContext: ExecutionContext): QueryResult = {
    import scala.concurrent.duration._

    val start = System.currentTimeMillis()
    val config = configuration
    val graphSONMode = GremlinUtils.getGraphSONMode(config.getString("graphson-mode"))

    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    // TODO - graph should provide backend to retrieve the stored table name in hbase
    val graphName = "iat_graph_" + graph.name

    // Create graph connection
    val titanConfiguration = GremlinUtils.getTitanConfiguration(config, "titan.query")
    titanConfiguration.setProperty("storage.tablename", "iat_graph_" + graph.name)
    val titanGraph: TitanGraph = getTitanGraph(graphName, titanConfiguration)
    val bindings = gremlinExecutor.createBindings()
    bindings.put("g", titanGraph)

    // Get results
    val resultIterator: Iterable[Any] = executeGremlinQuery(arguments.gremlin, bindings)
    val json = resultIterator.map(GremlinUtils.serializeGremlinToJson(titanGraph, _, graphSONMode))
    val time = (System.currentTimeMillis() - start).toDouble / 1000.0

    QueryResult(json, time)
  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[QueryArgs]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: QueryResult): JsObject = returnValue.toJson.asJsObject

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   */
  override def name: String = "graphs/query/gremlin"

  //TODO: Replace with generic code that works on any case class
  override def serializeArguments(arguments: QueryArgs): JsObject = arguments.toJson.asJsObject()

  /**
   * Gets Titan graph, and caches graph connection to reduce overhead of future accesses.
   *
   * @param graphName Name of graph
   * @param titanConfiguration Titan configuration for connecting to graph
   * @return Titan graph
   */
  private def getTitanGraph(graphName: String, titanConfiguration: SerializableBaseConfiguration): TitanGraph = {
    val titanConnector = new TitanGraphConnector(titanConfiguration)

    val titanGraph = titanGraphs.get(graphName).getOrElse({
      connectToTitanGraph(graphName, titanConnector)
    })

    titanGraph
  }

  private def connectToTitanGraph(graphName: String, titanConnector: TitanGraphConnector): TitanGraph = {
    GremlinQuery.lock.acquire()
    val titanGraph = titanConnector.connect()
    titanGraphs += graphName -> titanGraph
    GremlinQuery.lock.release()
    titanGraph
  }
}

object GremlinQuery {
  private val lock = new Lock()
}

