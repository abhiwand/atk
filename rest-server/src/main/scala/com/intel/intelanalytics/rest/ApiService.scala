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

package com.intel.intelanalytics.rest

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._
import akka.event.Logging
import com.intel.intelanalytics.rest.v1.ApiV1Service
import com.intel.intelanalytics.spray.json.IADefaultJsonProtocol

/**
 * We don't implement our route structure directly in the service actor because
 * we want to be able to test it independently, without having to spin up an actor
 *
 * @param apiService the service to delegate to
 */
class ApiServiceActor(val apiService: ApiService) extends Actor with HttpService {

  /**
   * the HttpService trait defines only one abstract member, which
   * connects the services environment to the enclosing actor or test
   */
  override def actorRefFactory = context

  /**
   * Delegates to apiService.
   *
   * This actor only runs our route, but you could add other things here, like
   * request stream processing or timeout handling
   */
  def receive = runRoute(apiService.serviceRoute)
}

/**
 * Defines our service behavior independently from the service actor
 */
class ApiService(val commonDirectives: CommonDirectives, val apiV1Service: ApiV1Service) extends Directives {

  def homepage = {
    respondWithMediaType(`text/html`) {
      complete {
        <html>
          <body>
            <h1>Welcome to the Intel Analytics Toolkit API Server</h1>
          </body>
        </html>
      }
    }
  }

  lazy val description = {
    new ServiceDescription(name = "Intel Analytics",
      identifier = RestServerConfig.identifier,
      versions = List("v1"))
  }

  lazy val oauthServer = { new OAuthServer(RestServerConfig.uaaUri) }

  import spray.json._
  import spray.httpx.SprayJsonSupport._
  import IADefaultJsonProtocol._
  implicit val descFormat = jsonFormat3(ServiceDescription)
  implicit val oauthServerFormat = jsonFormat1(OAuthServer)

  /**
   * Main Route entry point to the API Server
   */
  val serviceRoute: Route = logRequest("api service", Logging.InfoLevel) {
    path("") {
      get { homepage }
    } ~
      pathPrefix("v1") {
        apiV1Service.route
      } ~
      path("info") {
        respondWithMediaType(`application/json`) {
          commonDirectives.respondWithBuildId {
            complete(description)
          }
        }
      } ~
      path("oauth_server") {
        respondWithMediaType(`application/json`) {
          commonDirectives.respondWithBuildId {
            complete(oauthServer)
          }
        }
      }
  }
}

case class ServiceDescription(name: String, identifier: String, versions: List[String])
case class OAuthServer(uri: String)
