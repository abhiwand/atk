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
import com.intel.intelanalytics.service.v1.viewmodels._
import com.intel.intelanalytics.engine.{ Engine, EngineComponent }
import scala.concurrent._
import scala.util._
import com.intel.intelanalytics.service.v1.viewmodels.GetModel
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.model.{ ModelTemplate, ModelCreate, Model }
import com.intel.intelanalytics.domain.DomainJsonProtocol.DataTypeFormat
import com.intel.intelanalytics.service.{ ApiServiceConfig, CommonDirectives, AuthenticationDirective }
import spray.routing.Directives
import com.intel.intelanalytics.service.v1.decorators.ModelDecorator
import com.intel.intelanalytics.service.v1.viewmodels.ViewModelJsonImplicits
import com.intel.intelanalytics.service.v1.viewmodels.Rel
import com.intel.intelanalytics.spray.json.IADefaultJsonProtocol
import ExecutionContext.Implicits.global

import com.intel.event.EventLogging

/**
 * REST API Model Service
 */
class ModelService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  /**
   * The spray routes defining the Model service.
   */
  def modelRoutes() = {
    //import ViewModelJsonImplicits._
    val prefix = "models"

    /**
     * Creates "decorated model" for return on HTTP protocol
     * @param uri handle of model
     * @param model model metadata
     * @return Decorated model for HTTP protocol return
     */
    def decorate(uri: Uri, model: Model): GetModel = {
      //TODO: add other relevant links
      val links = List(Rel.self(uri.toString))
      ModelDecorator.decorateEntity(uri.toString, links, model)
    }

    commonDirectives(prefix) {
      implicit userProfile: UserPrincipal =>
        (path(prefix) & pathEnd) {
          requestUri {
            uri =>
              get {
                parameters('name.?) {
                  import spray.httpx.SprayJsonSupport._
                  implicit val indexFormat = ViewModelJsonImplicits.getModelFormat
                  (name) => name match {
                    case Some(name) => {
                      onComplete(engine.getModelByName(name)) {
                        case Success(Some(model)) => {
                          val links = List(Rel.self(uri.toString))
                          complete(ModelDecorator.decorateEntity(uri.toString(), links, model))
                        }
                        case _ => reject()
                      }
                    }
                    case _ =>
                      //TODO: cursor
                      onComplete(engine.getModels()) {
                        case Success(models) =>
                          import IADefaultJsonProtocol._
                          implicit val indexFormat = ViewModelJsonImplicits.getModelsFormat
                          complete(ModelDecorator.decorateForIndex(uri.toString(), models))
                        case Failure(ex) => throw ex
                      }
                  }
                }
              } ~
                post {
                  import spray.httpx.SprayJsonSupport._
                  implicit val format = DomainJsonProtocol.modelCreateFormat
                  implicit val indexFormat = ViewModelJsonImplicits.getModelFormat
                  entity(as[ModelCreate]) {
                    model =>
                      onComplete(engine.createModel(model)) {
                        case Success(model) => complete(decorate(uri + "/" + model.id, model))
                        case Failure(ex) => ctx => {
                          ctx.complete(500, ex.getMessage)
                        }
                      }
                  }
                }
          }
        } ~
          pathPrefix(prefix / LongNumber) {
            id =>
              pathEnd {
                requestUri {
                  uri =>
                    get {
                      onComplete(engine.getModel(id)) {
                        case Success(model) => {
                          val decorated = decorate(uri, model)
                          complete {
                            import spray.httpx.SprayJsonSupport._
                            implicit val format = DomainJsonProtocol.modelTemplateFormat
                            implicit val indexFormat = ViewModelJsonImplicits.getModelFormat
                            decorated
                          }
                        }
                        case _ => reject()
                      }
                    } ~
                      delete {
                        onComplete(for {
                          model <- engine.getModel(id)
                          res <- engine.deleteModel(model)
                        } yield res) {
                          case Success(ok) => complete("OK")
                          case Failure(ex) => throw ex
                        }
                      }
                }
              }
          }
    }

  }
}

