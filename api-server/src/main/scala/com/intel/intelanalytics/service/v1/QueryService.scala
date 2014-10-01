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

package com.intel.intelanalytics.service.v1

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.{ FilterPredicate, _ }
import com.intel.intelanalytics.domain.query.{ Query, Execution, QueryTemplate }
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.frame.load.{ Load, LoadSource }
import com.intel.intelanalytics.engine.Engine
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.service.v1.decorators.{ QueryDecorator }
import com.intel.intelanalytics.service.v1.viewmodels.ViewModelJsonImplicits._
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.service.{ ApiServiceConfig, CommonDirectives, UrlParser }
import spray.http.Uri
import scala.concurrent._
import spray.http.Uri
import spray.json._
import spray.routing.{ Directives, Route }

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success, Try }

import ExecutionContext.Implicits.global
import com.intel.event.EventLogging

/**
 * REST API Query Service
 */
class QueryService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  /**
   * Creates a view model for return through the HTTP protocol
   *
   * @param uri The link representing the command.
   * @param query The query being decorated
   * @return View model of the command.
   */
  def decorate(uri: Uri, query: Query, partition: Option[Long])(implicit user: UserPrincipal): GetQuery = {
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
    commonDirectives(prefix) { implicit principal: UserPrincipal =>
      pathPrefix(prefix / LongNumber) {
        id =>
          {
            requestUri {
              uri =>
                pathEnd {
                  get {
                    onComplete(engine.getQuery(id)) {
                      case Success(Some(query)) => complete(decorate(uri, query, None))
                      case _ => reject()
                    }
                  }
                } ~
                  pathPrefix("data") {
                    pathEnd {
                      get {
                        import ViewModelJsonImplicits._
                        onComplete(engine.getQuery(id)) {
                          case Success(Some(query)) => complete(QueryDecorator.decoratePages(uri.toString, query))
                          case _ => reject()
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
                              complete(if (query.complete) {
                                val result = engine.getQueryPage(query.id, page - 1)
                                QueryDecorator.decoratePage(uri.toString, links, query, page, dataToJson(result.data), result.schema)
                              }
                              else {
                                QueryDecorator.decorateEntity(uri.toString(), links, query)
                              })
                            case _ => reject()
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
                  onComplete(engine.getQueries(0, ApiServiceConfig.defaultCount)) {
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
