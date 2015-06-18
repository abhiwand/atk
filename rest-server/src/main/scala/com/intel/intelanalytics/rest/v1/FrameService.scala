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

import com.intel.intelanalytics.DuplicateNameException
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.domain.query.{ PagedQueryResult, QueryDataResult, RowQuery }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.rest.threading.SprayExecutionContext
import spray.json._
import spray.http.{ StatusCodes }
import com.intel.intelanalytics.rest.v1.viewmodels._
import com.intel.intelanalytics.engine.{ Engine }
import scala.concurrent._
import scala.util._
import com.intel.intelanalytics.rest.{ CommonDirectives }
import spray.routing.{ Directives }
import org.apache.commons.lang.StringUtils
import com.intel.intelanalytics.spray.json.IADefaultJsonProtocol
import com.intel.intelanalytics.rest.v1.decorators.{ QueryDecorator, FrameDecorator }

import scala.util.matching.Regex
import com.intel.event.EventLogging

import SprayExecutionContext.global

/**
 * REST API Data Frame Service.
 *
 * Always use onComplete( Future { operationsGoHere() } ) to prevent "server disconnected" messages in client.
 */
class FrameService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  def frameRoutes() = {
    val prefix = "frames"

    commonDirectives(prefix) { implicit invocation: Invocation =>
      (path(prefix) & pathEnd) {
        requestUri { uri =>
          get {
            parameters('name.?) {
              import spray.httpx.SprayJsonSupport._
              implicit val indexFormat = ViewModelJsonImplicits.getDataFrameFormat
              (name) => name match {
                case Some(name) => {
                  onComplete(engine.getFrameByName(name)) {
                    case Success(Some(frame)) => {
                      // uri comes in looking like /frames?name=abc
                      val baseUri = StringUtils.substringBeforeLast(uri.toString(), "/")
                      complete(FrameDecorator.decorateEntity(baseUri + "/" + frame.id, Nil, frame))
                    }
                    case Success(None) => complete(StatusCodes.NotFound, s"Frame with name '$name' was not found.")
                    case Failure(ex) => throw ex
                  }
                }
                case _ =>
                  onComplete(engine.getFrames()) {
                    case Success(frames) =>
                      import IADefaultJsonProtocol._
                      implicit val indexFormat = ViewModelJsonImplicits.getDataFramesFormat
                      complete(FrameDecorator.decorateForIndex(uri.toString(), frames))
                    case Failure(ex) => throw ex
                  }
              }
            }
          } ~
            post {
              import spray.httpx.SprayJsonSupport._
              implicit val format = DomainJsonProtocol.createEntityArgsFormat
              implicit val indexFormat = ViewModelJsonImplicits.getDataFrameFormat
              entity(as[CreateEntityArgs]) {
                createEntityArgs =>
                  onComplete(engine.createFrame(createEntityArgs)) {
                    case Success(createdFrame) => complete(FrameDecorator.decorateEntity(uri + "/" + createdFrame.id, Nil, createdFrame))
                    case Failure(ex: DuplicateNameException) => ctx => {
                      ctx.complete(202, ex.getMessage)
                    }
                    case Failure(ex) => ctx => {
                      ctx.complete(500, ex.getMessage)
                    }
                  }
              }
            }

        }
      } ~
        pathPrefix(prefix / LongNumber) { id =>
          requestUri { uri =>
            pathEnd {
              get {
                onComplete(engine.getFrame(id)) {
                  case Success(Some(frame)) => {
                    val decorated = FrameDecorator.decorateEntity(uri.toString(), Nil, frame)
                    complete {
                      import spray.httpx.SprayJsonSupport._
                      implicit val format = DomainJsonProtocol.dataFrameTemplateFormat
                      implicit val indexFormat = ViewModelJsonImplicits.getDataFrameFormat
                      decorated
                    }
                  }
                  case Success(None) => complete(StatusCodes.NotFound, "Frame was not found")
                  case Failure(ex) => throw ex
                }
              } ~
                delete {
                  onComplete(engine.deleteFrame(id)) {
                    case Success(_) => complete("OK")
                    case Failure(ex) => throw ex
                  }
                }
            } ~ (path("data") & get) {
              parameters('offset.as[Long], 'count.as[Long]) {
                (offset, count) =>
                  {
                    import ViewModelJsonImplicits._
                    val queryArgs = RowQuery[Long](id, offset, count)
                    onComplete(Future { engine.getRows(queryArgs) }) {
                      case Success(r: QueryDataResult) => {
                        complete(GetQuery(id = None, error = None,
                          name = "getRows", arguments = None, complete = true,
                          result = Some(GetQueryPage(
                            Some(dataToJson(r.data)), None, None, r.schema)),
                          links = List(Rel.self(uri.toString))))
                      }
                      case Success(exec: PagedQueryResult) => {
                        val pattern = new Regex(prefix + ".*")
                        val commandUri = pattern.replaceFirstIn(uri.toString, QueryService.prefix + "/") + exec.execution.start.id
                        complete(QueryDecorator.decorateEntity(commandUri, List(Rel.self(commandUri)), exec.execution.start, exec.schema))
                      }
                      case Failure(ex) => throw ex
                    }
                  }
              }
            }
          }
        }
    }
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
}
