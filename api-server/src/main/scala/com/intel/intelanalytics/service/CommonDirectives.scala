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

package com.intel.intelanalytics.service

import akka.event.Logging
import spray.http.StatusCodes
import com.intel.intelanalytics.security.UserPrincipal
import spray.routing._
import scala.util.control.NonFatal
import spray.http.HttpHeaders.RawHeader

/**
 * Directives common to all services
 *
 * @param authenticationDirective implementation for authentication
 */
class CommonDirectives(val authenticationDirective: AuthenticationDirective) extends Directives with EventLoggingDirectives {

  /**
   * Directives common to all services
   * @param eventCtx name of the current context for logging
   * @return directives with authenticated user
   */
  def apply(eventCtx: String): Directive1[UserPrincipal] = {
    eventContext(eventCtx) &
      addCommonResponseHeaders &
      handleExceptions(errorHandler) &
      logResponse(eventCtx, Logging.InfoLevel) &
      authenticationDirective.authenticateKey
  }

  def errorHandler = {
    ExceptionHandler {
      case e: IllegalArgumentException => {
        error("An error occurred during request processing.", exception = e)
        complete(StatusCodes.BadRequest, "Bad request: " + e.getMessage)
      }
      case NonFatal(e) => {
        error("An error occurred during request processing.", exception = e)
        complete(StatusCodes.InternalServerError, "An internal server error occurred")
      }
    }
  }

  /**
   * Adds header fields common to all responses
   * @return directive to wrap route with headers
   */
  def addCommonResponseHeaders: Directive0 =
    mapInnerRoute {
      route => respondWithBuildId { route }
    }

  def respondWithBuildId = respondWithHeader(RawHeader("build_id", ApiServiceConfig.buildId))

}
