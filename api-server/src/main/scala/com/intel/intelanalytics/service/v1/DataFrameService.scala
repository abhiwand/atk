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

import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.domain.query.{ PagedQueryResult, QueryDataResult, Query, RowQuery }
import org.joda.time.DateTime
import spray.json._
import spray.http.{ StatusCodes, HttpResponse, Uri }
import scala.Some
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.engine.{ Engine, EngineComponent }
import scala.concurrent._
import scala.util._
import com.intel.intelanalytics.service.v1.viewmodels.GetDataFrame
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.frame.DataFrameTemplate
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.domain.DomainJsonProtocol.DataTypeFormat
import com.intel.intelanalytics.service.{ ApiServiceConfig, CommonDirectives, AuthenticationDirective }
import spray.routing.Directives
import com.intel.intelanalytics.service.v1.decorators.FrameDecorator
import org.apache.commons.lang.StringUtils
import com.intel.intelanalytics.spray.json.IADefaultJsonProtocol
import com.intel.intelanalytics.service.v1.decorators.{ QueryDecorator, CommandDecorator, FrameDecorator }

import scala.util.matching.Regex
import com.intel.event.EventLogging

//TODO: Is this right execution context for us?
import ExecutionContext.Implicits.global

/**
 * REST API Data Frame Service
 */
class DataFrameService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  def frameRoutes() = {
    val prefix = "frames"

    commonDirectives(prefix) { implicit p: UserPrincipal =>
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
                    case _ => reject()
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
              implicit val format = DomainJsonProtocol.dataFrameTemplateFormat
              implicit val indexFormat = ViewModelJsonImplicits.getDataFrameFormat
              entity(as[DataFrameTemplate]) {
                frame =>
                  onComplete(engine.create(frame)) {
                    case Success(createdFrame) => complete(FrameDecorator.decorateEntity(uri + "/" + createdFrame.id, Nil, createdFrame))
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
                  case _ => reject()
                }
              } ~
                delete {
                  onComplete(for {
                    fopt <- engine.getFrame(id)
                    res <- fopt.map(f => engine.delete(f)).getOrElse(Future.successful(()))
                  } yield res) {
                    case Success(_) => complete("OK")
                    case Failure(ex) => throw ex
                  }
                }
            } ~ (path("data") & get) {
              parameters('offset.as[Int], 'count.as[Int]) {
                (offset, count) =>
                  {
                    import ViewModelJsonImplicits._
                    val queryArgs = RowQuery[Long](id, offset, count)
                    val results = engine.getRows(queryArgs)
                    results match {
                      case r: QueryDataResult => {
                        complete(GetQuery(id = None, error = None,
                          name = "getRows", arguments = None, complete = true,
                          result = Some(GetQueryPage(
                            Some(dataToJson(r.data)), None, None, r.schema)),
                          links = List(Rel.self(uri.toString))))
                      }
                      case exec: PagedQueryResult => {
                        val pattern = new Regex(prefix + ".*")
                        val commandUri = pattern.replaceFirstIn(uri.toString, QueryService.prefix + "/") + exec.execution.start.id
                        complete(QueryDecorator.decorateEntity(commandUri, List(Rel.self(commandUri)), exec.execution.start, exec.schema))
                      }
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
