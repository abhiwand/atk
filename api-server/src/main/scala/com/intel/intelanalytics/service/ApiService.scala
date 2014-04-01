package com.intel.intelanalytics.service

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._
import com.intel.intelanalytics.domain.Schema
import akka.event.Logging
import spray.routing.directives.BasicDirectives
import com.intel.event.{Severity, EventContext}
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.service.v1.ApiV1Service

// !!! IMPORTANT, else `convertTo` and `toJson` won't work correctly

// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class ApiServiceActor extends Actor with HttpService { this: ApiService =>

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  override def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(this.serviceRoute)
}


// this trait defines our service behavior independently from the service actor

trait
ApiService extends Directives
                        with EventLoggingDirectives { this: ApiService with ApiV1Service =>

  def homepage = {respondWithMediaType(`text/html`) {
      complete {
        <html>
          <body>
            <h1>Welcome to the Intel Analytics Toolkit API Server</h1>
          </body>
        </html>
      }
    }
  }

  val serviceRoute: Route = logRequest("api service", Logging.InfoLevel) {
    path("") {
      get { homepage }
    } ~
    pathPrefix("v1") {
      this.apiV1Service
    }
  }
}

trait EventLoggingDirectives extends EventLogging {
  import BasicDirectives._
  def eventContext(context: String): Directive0 =
    mapRequestContext { ctx ⇒
      withContext(context) {
        ctx.withRouteResponseMapped {
          response ⇒ response
        }
      }
    }
}
