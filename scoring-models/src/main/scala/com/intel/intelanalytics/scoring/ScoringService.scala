//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.scoring

import akka.actor.Actor
import com.intel.intelanalytics.domain.{ DomainJsonProtocol }
import spray.routing._
import spray.http._
import MediaTypes._
import akka.event.Logging
import scala.concurrent._
import ExecutionContext.Implicits.global
import com.intel.intelanalytics.spray.json.IADefaultJsonProtocol
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
class ScoringService(model: Model) extends Directives {

  def homepage = {
    respondWithMediaType(`text/html`) {
      complete {
        <html>
          <body>
            <h1>Welcome to the Intel Analytics Toolkit Scoring Server -2</h1>
          </body>
        </html>
      }
    }
  }

  lazy val description = {
    new ServiceDescription(name = "Intel Analytics",
      identifier = "ia",
      versions = List("v1"))
  }

  import IADefaultJsonProtocol._
  implicit val descFormat = jsonFormat3(ServiceDescription)

  /**
   * Main Route entry point to the Scoring Server
   */
  val serviceRoute: Route = logRequest("scoring service", Logging.InfoLevel) {
    val prefix = "models"
    path("") {
      get { homepage }
    } ~
      path("v1" / prefix / model.name) {
        requestUri { uri =>
          post {
            parameters('data.?) {
              (data) =>
                data match {
                  case Some(x) => {
                    onComplete(model.score(Seq(x))) {
                      case Success(scored) => complete(scored.toString)
                      case Failure(ex) => ctx => {
                        ctx.complete(StatusCodes.InternalServerError, ex.getMessage)
                      }
                    }
                  }
                  case None => reject()
                }
            }
          }
        }
      }
  }
}
case class ServiceDescription(name: String, identifier: String, versions: List[String])
