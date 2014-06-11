package com.intel.intelanalytics.service

import com.intel.intelanalytics.shared.EventLogging
import spray.routing.directives.BasicDirectives
import spray.routing._

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