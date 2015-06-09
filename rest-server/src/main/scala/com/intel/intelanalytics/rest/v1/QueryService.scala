/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.rest.v1

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.{ FilterArgs, _ }
import com.intel.intelanalytics.domain.query.{ Query, Execution, QueryTemplate }
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.frame.load.{ LoadFrameArgs, LoadSource }
import com.intel.intelanalytics.engine.Engine
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.rest.v1.decorators.QueryDecorator
import com.intel.intelanalytics.rest.v1.viewmodels.ViewModelJsonImplicits._
import com.intel.intelanalytics.rest.v1.viewmodels._
import com.intel.intelanalytics.rest.{ RestServerConfig, CommonDirectives, UrlParser }
import spray.http.{ StatusCodes, Uri }
import scala.concurrent._
import spray.json._
import spray.routing.{ Directives, Route }

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success, Try }

import ExecutionContext.Implicits.global
import com.intel.event.EventLogging

/**
 * REST API Query Service.
 *
 * Always use onComplete( Future { operationsGoHere() } ) to prevent "server disconnected" messages in client.
 */
class QueryService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  /**
   * Creates a view model for return through the HTTP protocol
   *
   * @param uri The link representing the command.
   * @param query The query being decorated
   * @return View model of the command.
   */
  def decorate(uri: Uri, query: Query, partition: Option[Long])(implicit invocation: Invocation): GetQuery = {
    //TODO: add other relevant links
    val links = List(Rel.self(uri.toString()))
    QueryDecorator.decorateEntity(uri.toString(), links, query)
  }

  /**
   * Convert an Iterable of Any to a List of JsValue. Required due to how spray-json handles AnyVals
   * @param data iterable to return in response
   * @return JSON friendly version of data
   */
  def dataToJson(data: Iterable[Array[Any]]): List[JsValue] = {
    import com.intel.intelanalytics.domain.DomainJsonProtocol._
    data.map(row => row.map {
      case null => JsNull
      case a => a.toJson
    }.toJson).toList
  }

  val prefix = QueryService.prefix

  /**
   * The spray routes defining the query service.
   */
  def queryRoutes() = {
    commonDirectives(prefix) { implicit invocation: Invocation =>
      pathPrefix(prefix / LongNumber) {
        id =>
          {
            requestUri {
              uri =>
                pathEnd {
                  get {
                    onComplete(engine.getQuery(id)) {
                      case Success(Some(query)) => complete(decorate(uri, query, None))
                      case Success(None) => complete(StatusCodes.NotFound)
                      case Failure(ex) => throw ex
                    }
                  }
                } ~
                  pathPrefix("data") {
                    pathEnd {
                      get {
                        import ViewModelJsonImplicits._
                        onComplete(engine.getQuery(id)) {
                          case Success(Some(query)) => complete(QueryDecorator.decoratePages(uri.toString(), query))
                          case Success(None) => complete(StatusCodes.NotFound)
                          case Failure(ex) => throw ex
                        }
                      }
                    }
                  } ~ pathPrefix("data" / IntNumber) {
                    page =>
                      {
                        get {

                          val links = List(Rel.self(uri.toString()))
                          onComplete(engine.getQuery(id)) {
                            case Success(Some(query)) =>
                              if (query.complete) {
                                onComplete(Future { engine.getQueryPage(query.id, page - 1) }) {
                                  case Success(result) => complete(QueryDecorator.decoratePage(uri.toString(), links, query, page, dataToJson(result.data), result.schema))
                                  case Failure(ex) => throw ex
                                }
                              }
                              else {
                                complete(QueryDecorator.decorateEntity(uri.toString(), links, query))
                              }
                            case Success(None) => complete(StatusCodes.NotFound)
                            case Failure(ex) => throw ex
                          }
                        }
                      }
                  }
            }
          }
      } ~
        (path(prefix)) {
          requestUri {
            uri =>
              pathEnd {
                get {
                  import spray.json._
                  import ViewModelJsonImplicits._
                  //TODO: cursor
                  onComplete(engine.getQueries(0, RestServerConfig.defaultCount)) {
                    case Success(queries) => complete(QueryDecorator.decorateForIndex(uri.toString(), queries))
                    case Failure(ex) => throw ex
                  }
                }
              }
          }
        }
    }
  }
}

object QueryService {
  val prefix = "queries"
}
