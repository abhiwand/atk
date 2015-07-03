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

package com.intel.taproot.analytics.rest.v1

import com.intel.taproot.analytics.domain._
import com.intel.taproot.analytics.engine.plugin.Invocation
import com.intel.taproot.analytics.rest.threading.SprayExecutionContext
import spray.http.{ StatusCodes, Uri }
import com.intel.taproot.analytics.engine.{ Engine }
import scala.concurrent._
import scala.util._
import com.intel.taproot.analytics.rest.v1.viewmodels.GetModel
import com.intel.taproot.analytics.domain.model.{ ModelEntity }
import com.intel.taproot.analytics.rest.{ CommonDirectives }
import spray.routing.Directives
import com.intel.taproot.analytics.rest.v1.decorators.ModelDecorator
import com.intel.taproot.analytics.rest.v1.viewmodels.ViewModelJsonImplicits
import com.intel.taproot.analytics.rest.v1.viewmodels.Rel
import com.intel.taproot.analytics.spray.json.IADefaultJsonProtocol
import SprayExecutionContext.global
import com.intel.taproot.event.EventLogging
import spray.json._

/**
 * REST API Model Service.
 *
 * Always use onComplete( Future { operationsGoHere() } ) to prevent "server disconnected" messages in client.
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
    def decorate(uri: Uri, model: ModelEntity): GetModel = {
      //TODO: add other relevant links
      val links = List(Rel.self(uri.toString))
      ModelDecorator.decorateEntity(uri.toString, links, model)
    }

    commonDirectives(prefix) {
      implicit invocation: Invocation =>
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
                        case Success(None) => complete(StatusCodes.NotFound, s"Model with name '$name' was not found.")
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
                  implicit val format = DomainJsonProtocol.createEntityArgsFormat
                  implicit val indexFormat = ViewModelJsonImplicits.getModelFormat
                  entity(as[CreateEntityArgs]) {
                    createArgs =>
                      onComplete(engine.createModel(createArgs)) {
                        case Success(model) => complete(decorate(uri + "/" + model.id, model))
                        case Failure(ex) => ctx => {
                          ctx.complete(StatusCodes.InternalServerError, ex.getMessage)
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
                        onComplete(engine.deleteModel(id)) {
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
