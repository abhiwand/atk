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

package com.intel.intelanalytics.engine.spark.queries

import com.intel.intelanalytics.component.{ ArchiveName, Boot }
import com.intel.intelanalytics.domain.query.{ Query, QueryTemplate, Execution }
import com.intel.intelanalytics.engine.plugin.{ QueryPluginResults, FunctionQuery, QueryPlugin }
import com.intel.intelanalytics.engine.spark.context.SparkContextManager
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.engine.spark.{ SparkEngine, SparkEngineConfig }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.{ ClassLoaderAware, NotFoundException }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spray.json._

import scala.concurrent._
import scala.util.Try

/**
 * QueryExecutor manages a registry of QueryPlugins and executes them on request.
 *
 * The plugin registry is based on configuration - all Archives listed in the configuration
 * file under intel.analytics.engine.archives will be queried for the "QueryPlugin" key, and
 * any plugins they provide will be added to the plugin registry.
 *
 * Plugins can also be added programmatically using the registerQuery method.
 *
 * Plugins can be executed in three ways:
 *
 * 1. A QueryPlugin can be passed directly to the execute method. The query need not be in
 *    the registry
 * 2. A query can be called by name. This requires that the query be in the registry.
 * 3. A query can be called with a QueryTemplate. This requires that the query named by
 *    the query template be in the registry, and that the arguments provided in the QueryTemplate
 *    can be parsed by the query.
 *
 * @param engine an Engine instance that will be passed to query plugins during execution
 * @param queries a query storage that the executor can use for audit logging query execution
 * @param contextManager a SparkContext factory that can be passed to SparkQueryPlugins during execution
 */
class QueryExecutor(engine: => SparkEngine, queries: SparkQueryStorage, contextManager: SparkContextManager)
    extends EventLogging
    with ClassLoaderAware {

  private var queryPlugins: Map[String, QueryPlugin[_]] = SparkEngineConfig.archives.flatMap {
    case (archive, className) => Boot.getArchive(ArchiveName(archive, className))
      .getAll[QueryPlugin[_]]("QueryPlugin")
      .map(p => (p.name, p))
  }.toMap

  /**
   * Registers a function as a query using FunctionQuery. This is a convenience method,
   * it is also possible to construct a FunctionQuery explicitly and pass it to the
   * registerQuery method that takes a CommandPlugin.
   *
   * @param name the name of the query
   * @param function the function to be called when running the query
   * @tparam A the argument type of the query
   * @return the QueryPlugin instance created during the registration process.
   */
  def registerQuery[A <: Product: JsonFormat: ClassManifest](name: String,
                                                             function: (A, UserPrincipal) => Any): QueryPlugin[A] =
    registerQuery(FunctionQuery(name, function))

  /**
   * Adds the given query to the registry.
   * @param query the query to add
   * @tparam A the argument type for the query
   * @return the same query that was passed, for convenience
   */
  def registerQuery[A <: Product: ClassManifest](query: QueryPlugin[A]): QueryPlugin[A] = {
    synchronized {
      queryPlugins += (query.name -> query)
    }
    query
  }

  private def getQueryDefinition(name: String): Option[QueryPlugin[_]] = {
    queryPlugins.get(name)
  }

  /**
   * Executes the given query template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the query execution back in the persistent query object.
   *
   * @param query the query to run, including name and arguments
   * @param user the user running the query
   * @return an Execution object that can be used to track the query's execution
   */
  def execute[A <: Product: ClassManifest](query: QueryPlugin[A],
                                           arguments: A,
                                           user: UserPrincipal,
                                           executionContext: ExecutionContext): Execution = {
    implicit val ec = executionContext
    val q = queries.create(QueryTemplate(query.name, Some(query.serializeArguments(arguments))))
    withMyClassLoader {
      withContext("ce.execute") {
        withContext(query.name) {
          val context: SparkContext = contextManager.context(user).sparkContext
          val cmdFuture = future {
            withQuery(q) {
              import com.intel.intelanalytics.domain.DomainJsonProtocol._
              val invocation: SparkInvocation = SparkInvocation(engine, commandId = q.id, arguments = q.arguments,
                user = user, executionContext = implicitly[ExecutionContext],
                sparkContext = context)

              context.setLocalProperty("command-id", q.id.toString)
              context.setLocalProperty("command-type", "QueryPlugin")

              val funcResult = query(invocation, arguments)

              val rdd = funcResult match {
                case x: RDD[Any] => x
                case x: Seq[Any] => context.parallelize(x)
                case x: Iterable[Any] => context.parallelize(x.toSeq)
                case _ => ???
              }
              val location = queries.getAbsoluteFrameDirectory(q.id)

              rdd.saveAsObjectFile(location)
              val pageSize = SparkEngineConfig.pageSize
              val totalPages = math.ceil(rdd.count().toDouble / pageSize).toInt
              QueryPluginResults(totalPages, pageSize).toJson.asJsObject()
            }
            queries.lookup(q.id).get
          }
          Execution(q, cmdFuture)
        }
      }
    }
  }

  /**
   * Executes the given query template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the query execution back in the persistent query object.
   *
   * This overload requires that the query already is registered in the plugin registry using registerQuery.
   *
   * @param name the name of the query to run
   * @param arguments the arguments to pass to the query
   * @param user the user running the query
   * @return an Execution object that can be used to track the query's execution
   */
  def execute[A <: Product: ClassManifest](name: String,
                                           arguments: A,
                                           user: UserPrincipal,
                                           executionContext: ExecutionContext): Execution = {
    val function = getQueryDefinition(name)
      .getOrElse(throw new NotFoundException("query definition", name))
      .asInstanceOf[QueryPlugin[A]]
    execute(function, arguments, user, executionContext)
  }

  /**
   * Executes the given query template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the query execution back in the persistent query object.
   *
   * This overload requires that the query already is registered in the plugin registry using registerQuery.
   *
   * @param query the QueryTemplate from which to extract the query name and the arguments
   * @param user the user running the query
   * @return an Execution object that can be used to track the query's execution
   */
  def execute[A <: Product: ClassManifest](query: QueryTemplate,
                                           user: UserPrincipal,
                                           executionContext: ExecutionContext): Execution = {
    val function = getQueryDefinition(query.name)
      .getOrElse(throw new NotFoundException("query definition", query.name))
      .asInstanceOf[QueryPlugin[A]]
    val convertedArgs = function.parseArguments(query.arguments.get)
    execute(function, convertedArgs, user, executionContext)
  }

  private def withQuery[T](query: Query)(block: => JsObject): Unit = {
    queries.complete(query.id, Try {
      block
    })

  }
}