package com.intel.intelanalytics.engine.spark.graph.query

import javax.script.Bindings

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.graph.GraphName
import com.intel.intelanalytics.security.UserPrincipal
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.{ Await, ExecutionContext, Lock }
import scala.util.Try
import com.intel.intelanalytics.domain.command.CommandDoc

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

/** Json conversion for arguments and return value case classes */
object GremlinQueryFormat {
  import com.intel.intelanalytics.domain.DomainJsonProtocol._
  implicit val queryArgsFormat = jsonFormat2(QueryArgs)
  implicit val queryResultFormat = jsonFormat2(QueryResult)
}

import GremlinQueryFormat._

/**
 * Command plugin for executing Gremlin queries.
 */
class GremlinQuery extends CommandPlugin[QueryArgs, QueryResult] {

  val gremlinExecutor = new GremlinGroovyScriptEngine()
  var titanGraphs = Map[String, TitanGraph]()

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   */
  override def name: String = "graphs/query/gremlin"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc = Some(CommandDoc(oneLineSummary = "Executes a Gremlin query.",
    extendedSummary = Some("""
    Extended Summary
    ----------------
    Executes a Gremlin query on an existing graph.

    The query returns a list of results in GraphSON format(for vertices or edges)
    or JSON (for other results like counts). GraphSON is a JSON-based format for
    property graphs which uses reserved keys that begin with underscores to
    encode vertex and edge metadata.

    Parameters
    ----------
    gremlin: String
        The Gremlin script to execute.
        At present, the query does not support pagination so the results of query
        should be limited using the Gremlin range filter [i..j], e.g., g.V[0..9]
        to return the first 10 vertices.

    Raises
    ------
    RuntimeException
      	If the Gremlin script could not be executed due to invalid syntax.

    Returns
    -------
    Dictionary
        Query results and runtime in seconds.


    Examples
    --------
    Get the first two outgoing edges of the vertex whose source equals 5767244
      	mygraph = BigGraph(...)
      	results = mygraph.query.gremlin("g.V('source', 5767244).outE[0..1]")
      	print results["results"]

    The expected output is a list of edges in GraphSON format:
    	  [{u'_label': u'edge', u'_type': u'edge', u'_inV': 1381202500, u'weight': 1, u'_outV': 1346400004, u'_id': u'fDEQC9-1t7m96-1U'},
    	  {u'_label': u'edge', u'_type': u'edge', u'_inV': 1365600772, u'weight': 1, u'_outV': 1346400004, u'_id': u'frtzv9-1t7m96-1U'}]

    Get the count of incoming edges for a vertex.
    	results = mygraph.query.gremlin("g.V('target', 5767243).inE.count()")
    	print results["results"]

    The expected output is:
    	[4]
""")))

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

    // Create graph connection
    val titanConfiguration = SparkEngineConfig.createTitanConfiguration(config, "titan.query")
    val titanTableNameKey = TitanGraphConnector.getTitanTableNameKey(titanConfiguration)
    val iatGraphName = GraphName.convertGraphUserNameToBackendName(graph.name)
    titanConfiguration.setProperty(titanTableNameKey, iatGraphName)

    val titanGraph = getTitanGraph(iatGraphName, titanConfiguration)
    val bindings = gremlinExecutor.createBindings()
    bindings.put("g", titanGraph)

    // Get results
    val resultIterator = executeGremlinQuery(titanGraph, arguments.gremlin, bindings, graphSONMode)
    val runtimeInSeconds = (System.currentTimeMillis() - start).toDouble / 1000.0

    QueryResult(resultIterator, runtimeInSeconds)
  }

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
    import com.intel.intelanalytics.domain.DomainJsonProtocol._

    val results = Try(gremlinExecutor.eval(gremlinScript, bindings)).getOrElse({
      throw new RuntimeException(s"Invalid syntax for Gremlin query: ${gremlinScript}")
    })

    val resultIterator = results match {
      case x: java.lang.Iterable[_] => x.toIterable
      case x => List(x).toIterable
    }

    val jsResultsIterator = Try({
      resultIterator.filter(x => x != null).map(GremlinUtils.serializeGremlinToJson(titanGraph, _, graphSONMode))
    }).getOrElse({
      throw new RuntimeException(s"Invalid syntax for Gremlin query: ${gremlinScript}")
    })

    jsResultsIterator
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

    val titanGraph = titanGraphs.getOrElse(graphName, connectToTitanGraph(graphName, titanConnector))

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

