package com.intel.intelanalytics.service

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._
import akka.event.Logging
import spray.routing.directives.BasicDirectives
import com.intel.event.{ Severity, EventContext }
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.service.v1.ApiV1Service
import com.intel.intelanalytics.service.v1.viewmodels.RelLink
import com.typesafe.config.ConfigFactory
import com.intel.intelanalytics.domain.schema.Schema

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
class ApiService(val apiV1Service: ApiV1Service) extends Directives with EventLoggingDirectives {

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

  private val config = ConfigFactory.load()

  lazy val description = {
    new ServiceDescription(name = "Intel Analytics",
      identifier = config.getString("intel.analytics.api.identifier"),
      versions = List("v1"))
  }

  import spray.json._
  import spray.httpx.SprayJsonSupport._
  import DefaultJsonProtocol._
  implicit val descFormat = jsonFormat3(ServiceDescription)

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
          complete(description)
        }
      }
  }
}

case class ServiceDescription(name: String, identifier: String, versions: List[String])


