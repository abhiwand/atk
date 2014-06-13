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
import spray.http.Uri
import scala.Some
import com.intel.intelanalytics.repository.MetaStoreComponent
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.engine.{Engine, EngineComponent}
import scala.concurrent._
import scala.util._
import com.intel.intelanalytics.service.v1.viewmodels.GetDataFrame
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.frame.DataFrameTemplate
import com.intel.intelanalytics.domain.frame.DataFrame
import com.intel.intelanalytics.domain.DomainJsonProtocol.DataTypeFormat
import com.typesafe.config.ConfigFactory
import com.intel.intelanalytics.service.{CommonDirectives, AuthenticationDirective}
import spray.routing.Directives
import com.intel.intelanalytics.service.v1.decorators.FrameDecorator

//TODO: Is this right execution context for us?
import ExecutionContext.Implicits.global

/**
 * REST API Data Frame Service
 */
class DataFrameService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  val config = ConfigFactory.load()
  val defaultCount = config.getInt("intel.analytics.api.defaultCount")

  def frameRoutes() = {
    val prefix = "dataframes"

    def decorate(uri: Uri, frame: DataFrame): GetDataFrame = {
      //TODO: add other relevant links
      val links = List(Rel.self(uri.toString))
      FrameDecorator.decorateEntity(uri.toString, links, frame)
    }

    //TODO: none of these are yet asynchronous - they communicate with the engine
    //using futures, but they keep the client on the phone the whole time while they're waiting
    //for the engine work to complete. Needs to be updated to a) register running jobs in
    //
    //
    //
    //
    // the metastore
    //so they can be queried, and b) support the web hooks.
    commonDirectives(prefix) { implicit p: UserPrincipal =>
      (path(prefix) & pathEnd) {
        requestUri { uri =>
          get {
            import spray.json._
            import ViewModelJsonImplicits._
            //TODO: cursor
            onComplete(engine.getFrames(0, defaultCount)) {
              case Success(frames) => complete(FrameDecorator.decorateForIndex(uri.toString(), frames))
              case Failure(ex) => throw ex
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
                    case Failure(ex) => throw ex
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
                  onComplete(for { r <- engine.getRows(id, offset, count) } yield r) {
                    case Success(rows: Iterable[Array[Any]]) => {
                      import spray.httpx.SprayJsonSupport._
                      import spray.json._
                      import DomainJsonProtocol._
                      val strings = rows.map(r => r.map(a => a match {
                        case null => JsNull
                        case _ => a.toJson
                      }).toList).toList
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
