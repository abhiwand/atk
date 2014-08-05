package com.intel.intelanalytics.engine.spark.graph.query

import javax.script.Bindings

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.security.UserPrincipal
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.{ Await, ExecutionContext, Lock }
import scala.util.Try

/**
 * Arguments for Gremlin query.
 *
 * Examples of Gremlin queries:
 * g.V[0..9] - Returns the first 10 vertices in graph
 * g.V.userId - Returns the userId property from vertices
 * g.V('name','hercules').out('father').out('father').name - Returns the name of Hercules' grandfather
 *
 * @param graph Graph reference
 * @param gremlin Gremlin script to execute
 */
case class QueryArgs(graph: GraphReference, gremlin: String)

/**
 * Results of Gremlin query.
 *
 * The results of the Gremlin query are serialized to GraphSON (for vertices or edges) or JSON (for other results
 * like counts, property values). GraphSON is a JSON-based graph format for property graphs. GraphSON uses reserved
 * keys which begin with underscores to encode vertex and edge metadata.
 *
 * Examples of valid GraphSON:
 * { "name": "lop", "lang": "java","_id": "3", "_type": "vertex" }
 * { "weight": 1, "_id": "8", "_type": "edge", "_outV": "1",  "_inV": "4", "_label": "knows" }
 *
 * @see https://github.com/tinkerpop/blueprints/wiki/GraphSON-Reader-and-Writer-Library
 *
 * @param results Results of Gremlin query serialized to JSON
 * @param run_time_seconds Runtime of Gremlin query in seconds
 */
case class QueryResult(results: Iterable[JsValue], run_time_seconds: Double)

/**
 * Command plugin for executing Gremlin queries.
 */
class GremlinQuery extends CommandPlugin[QueryArgs, QueryResult] {

  import com.intel.intelanalytics.domain.DomainJsonProtocol._

  implicit val queryArgsFormat = jsonFormat2(QueryArgs)
  implicit val queryResultFormat = jsonFormat2(QueryResult)

  val gremlinExecutor = new GremlinGroovyScriptEngine()
  var titanGraphs = Map[String, TitanGraph]()

  /**
   * Executes a Gremlin query.
   *
   * @param invocation information about the user and the circumstances at the time of the call
   * @param arguments Gremlin script to execute
   * @param user Principal representing an authenticated API user
   * @param executionContext Execution context
   * @return Results of executing Gremlin query
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
    val titanConfiguration = SparkEngineConfig.createTitanConfiguration(config, "titan.query")

    titanConfiguration.setProperty("storage.tablename", "iat_graph_" + graph.name)
    val titanGraph = getTitanGraph(graphName, titanConfiguration)
    val bindings = gremlinExecutor.createBindings()
    bindings.put("g", titanGraph)

    // Get results
    val resultIterator = executeGremlinQuery(titanGraph, arguments.gremlin, bindings, graphSONMode)
    val runtimeInSeconds = (System.currentTimeMillis() - start).toDouble / 1000.0

    QueryResult(resultIterator, runtimeInSeconds)
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
   * Execute gremlin query.
   *
   * @param gremlinScript Gremlin query
   * @param bindings Bindings for Gremlin engine
   * @return Iterable of query results
   */
  def executeGremlinQuery(titanGraph: TitanGraph, gremlinScript: String,
                          bindings: Bindings,
                          graphSONMode: GraphSONMode = GraphSONMode.NORMAL): Iterable[JsValue] = {
    val results = Try(gremlinExecutor.eval(gremlinScript, bindings))
      .getOrElse(throw new RuntimeException(s"Could not execute Gremlin query: ${gremlinScript}"))

    val resultIterator = results match {
      case x: java.lang.Iterable[_] => x.toIterable
      case x => List(x).toIterable
    }
    resultIterator.map(GremlinUtils.serializeGremlinToJson(titanGraph, _, graphSONMode))

  }

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

  /**
   * Connects to a Titan Graph and caches it.
   * @param graphName Name of graph
   * @param titanConnector Titan graph connector
   * @return Titan graph
   */
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
