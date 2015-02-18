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

package com.intel.intelanalytics.engine.spark.graph.query

import javax.script.Bindings

import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandInvocation, CommandPlugin, Invocation }
import com.intel.intelanalytics.engine.spark.graph.GraphBuilderConfigFactory
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.util.{ Failure, Success, Try }
import com.typesafe.config.Config
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.engine.CommandStorageProgressUpdater

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

  /**
   * The name of the command, e.g. graph/sampling/vertex_sample
   */
  override def name: String = "graph:titan/query/gremlin"

  /**
   * Executes a Gremlin query.
   *
   * @param invocation information about the user and the circumstances at the time of the call
   * @param arguments Gremlin script to execute
   * @return Results of executing Gremlin query
   */
  override def execute(arguments: QueryArgs)(implicit invocation: Invocation): QueryResult = {
    import scala.concurrent.duration._

    invocation.updateProgress(5f)
    val start = System.currentTimeMillis()
    val config = configuration
    val graphSONMode = GremlinUtils.getGraphSONMode(config.getString("graphson-mode"))

    val graphFuture = engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)
    val titanGraph = getTitanGraph(graph.name.get, config)

    val resultIterator = Try({
      val bindings = gremlinExecutor.createBindings()
      bindings.put("g", titanGraph)
      val results = executeGremlinQuery(titanGraph, arguments.gremlin, bindings, graphSONMode)
      results
    })
    invocation.updateProgress(100f)

    titanGraph.shutdown()

    val runtimeInSeconds = (System.currentTimeMillis() - start).toDouble / 1000.0

    val queryResult = resultIterator match {
      case Success(iterator) => QueryResult(iterator, runtimeInSeconds)
      case Failure(exception) => throw exception
    }

    queryResult
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
   * Connects to Titan graph.
   *
   * @param graphName Name of graph
   * @param config Command configuration
   * @return Titan graph
   */
  private def getTitanGraph(graphName: String, config: Config): TitanGraph = {
    val titanConfiguration = GraphBuilderConfigFactory.getTitanConfiguration(config, "titan.query", graphName)
    val titanConnector = new TitanGraphConnector(titanConfiguration)
    val titanGraph = titanConnector.connect()
    titanGraph
  }

}
