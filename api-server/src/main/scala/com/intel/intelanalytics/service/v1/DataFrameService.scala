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
import spray.json._
import spray.http.{ StatusCodes, HttpResponse, Uri }
import scala.Some
import com.intel.intelanalytics.repository.MetaStoreComponent
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.engine.{ Engine, EngineComponent }
import scala.concurrent._
import scala.util._
import com.intel.intelanalytics.service.v1.viewmodels.GetDataFrame
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.frame.DataFrameTemplate
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.domain.DomainJsonProtocol.DataTypeFormat
import com.intel.intelanalytics.service.{ ApiServiceConfig, CommonDirectives, AuthenticationDirective }
import spray.routing.Directives
import com.intel.intelanalytics.service.v1.decorators.FrameDecorator

//TODO: Is this right execution context for us?
import ExecutionContext.Implicits.global

/**
 * REST API Data Frame Service
 */
class DataFrameService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  def frameRoutes() = {
    val prefix = "dataframes"

    def decorate(uri: Uri, frame: DataFrame): GetDataFrame = {
      //TODO: add other relevant links
      val links = List(Rel.self(uri.toString))
      FrameDecorator.decorateEntity(uri.toString, links, frame)
    }

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

                      val links = List(Rel.self(uri.toString))
                      complete(FrameDecorator.decorateEntity(uri.toString(), links, frame))
                    }
                    case _ => reject()
                  }
                }
                case _ =>
                  onComplete(engine.getFrames(0, ApiServiceConfig.defaultCount)) {
                    case Success(frames) =>
                      import DefaultJsonProtocol._
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
                    case Success(createdFrame) => complete(decorate(uri + "/" + createdFrame.id, createdFrame))
                    case Failure(ex) => ctx => {
                      ctx.complete(500, ex.getMessage)
                    }
                  }
              }
            }

        }
      } ~
        pathPrefix(prefix / LongNumber) { id =>
          pathEnd {
            requestUri { uri =>
              get {
                onComplete(engine.getFrame(id)) {
                  case Success(Some(frame)) => {
                    val decorated = decorate(uri, frame)
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
            }
          } ~
            (path("data") & get) {
              parameters('offset.as[Int], 'count.as[Int]) {
                (offset, count) =>
                  onComplete(engine.getRows(id, offset, count)) {
                    case Success(rows: Iterable[Array[Any]]) => {
                      import spray.httpx.SprayJsonSupport._
                      import spray.json._
                      import DomainJsonProtocol._
                      val strings = rows.map(r => r.map {
                        case null => JsNull
                        case a => a.toJson
                      }.toList).toList
                      complete(strings)
                    }
                    case Failure(ex) => throw ex
                  }
              }
            }
        }
    }
  }

}
