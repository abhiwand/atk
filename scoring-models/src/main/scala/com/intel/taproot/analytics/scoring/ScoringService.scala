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

package com.intel.taproot.analytics.scoring

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._
import akka.event.Logging
import scala.concurrent._
import ExecutionContext.Implicits.global
import com.intel.taproot.analytics.spray.json.AtkDefaultJsonProtocol
import scala.util.{ Failure, Success }

/**
 * We don't implement our route structure directly in the service actor because
 * we want to be able to test it independently, without having to spin up an actor
 *
 * @param scoringService the service to delegate to
 */
class ScoringServiceActor(val scoringService: ScoringService) extends Actor with HttpService {

  /**
   * the HttpService trait defines only one abstract member, which
   * connects the services environment to the enclosing actor or test
   */
  override def actorRefFactory = context

  /**
   * Delegates to Scoring Service.
   *
   * This actor only runs our route, but you could add other things here, like
   * request stream processing or timeout handling
   */
  def receive = runRoute(scoringService.serviceRoute)
}

/**
 * Defines our service behavior independently from the service actor
 */
class ScoringService(model: Model, modelName: String) extends Directives {

  def homepage = {
    respondWithMediaType(`text/html`) {
      complete {
        <html>
          <body>
            <h1>Welcome to the Scoring Engine -2</h1>
          </body>
        </html>
      }
    }
  }

  lazy val description = {
    new ServiceDescription(name = "TapRoot Analytics",
      identifier = "ia",
      versions = List("v1"))
  }

  import AtkDefaultJsonProtocol._
  implicit val descFormat = jsonFormat3(ServiceDescription)

  /**
   * Main Route entry point to the Scoring Server
   */
  val serviceRoute: Route = logRequest("scoring service", Logging.InfoLevel) {
    val prefix = "models"
    path("") {
      get {
        homepage
      }
    } ~
      path("v1" / prefix / modelName) {
        requestUri { uri =>
          post {
            parameterSeq { (params) =>
              val sr = params.toArray
              var records = Seq[Array[String]]()
              for (i <- 0 until sr.length) {
                val decoded = java.net.URLDecoder.decode(sr(i)._2, "UTF-8")
                val splitSegment = decoded.split(",")
                records = records :+ splitSegment
              }
              onComplete(model.score(records)) {
                case Success(scored) => complete(scored.toString)
                case Failure(ex) => ctx => {
                  ctx.complete(StatusCodes.InternalServerError, ex.getMessage)
                }
              }
            }
          }
        }
      }
  }
}
case class ServiceDescription(name: String, identifier: String, versions: List[String])
