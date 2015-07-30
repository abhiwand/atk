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

package com.intel.taproot.analytics.rest

import com.intel.taproot.analytics.event.EventContext
import com.intel.taproot.analytics.NotFoundException
import com.intel.taproot.analytics.engine.plugin.Invocation
import spray.http.HttpHeaders.RawHeader
import spray.http.{ HttpRequest, StatusCodes }
import spray.routing._
import spray.routing.directives.LoggingMagnet

import scala.util.control.NonFatal

/**
 * Directives common to all services
 *
 * @param authenticationDirective implementation for authentication
 */
class CommonDirectives(val authenticationDirective: AuthenticationDirective) extends Directives with EventLoggingDirectives {

  def logReqResp(contextName: String)(req: HttpRequest) = {
    //In case we're re-using a thread that already had an event context
    EventContext.setCurrent(null)
    val ctx = EventContext.enter(contextName)
    info(req.method.toString() + " " + req.uri.toString())
    (res: Any) => {
      EventContext.setCurrent(ctx)
      info("RESPONSE: " + res.toString())
      ctx.close()
    }
  }

  /**
   * Directives common to all services
   * @param eventCtx name of the current context for logging
   * @return directives with authenticated user
   */
  def apply(eventCtx: String): Directive1[Invocation] = {
    //eventContext(eventCtx) &
    logRequestResponse(LoggingMagnet(logReqResp(eventCtx))) &
      //      logRequest(LoggingMagnet((req: HttpRequest) => {
      //        EventContext.enter(eventCtx)
      //        info(req.method.toString() + " " + req.uri.toString())
      //      })) &
      //      logResponse(LoggingMagnet((res: Any) => {
      //        info("RESPONSE: " + res.toString())
      //      })) &
      addCommonResponseHeaders &
      handleExceptions(errorHandler) &
      authenticationDirective.authenticateKey
  }

  def errorHandler = {
    ExceptionHandler {
      case e: IllegalArgumentException =>
        error("An error occurred during request processing.", exception = e)
        complete(StatusCodes.BadRequest, "Bad request: " + e.getMessage)
      case e: NotFoundException =>
        error("An error occurred during request processing.", exception = e)
        complete(StatusCodes.NotFound, e.getMessage)
      case NonFatal(e) =>
        error("An error occurred during request processing.", exception = e)
        complete(StatusCodes.InternalServerError, "An internal server error occurred")
    }
  }

  /**
   * Adds header fields common to all responses
   * @return directive to wrap route with headers
   */
  def addCommonResponseHeaders(): Directive0 =
    mapInnerRoute {
      route =>
        respondWithBuildId {
          route
        }
    }

  def respondWithBuildId = respondWithHeader(RawHeader("build_id", RestServerConfig.buildId))

}
