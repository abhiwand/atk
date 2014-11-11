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

//TODO: Is this right execution context for us?

import spray.http.HttpHeader

import scala.PartialFunction._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import com.intel.intelanalytics.security.UserPrincipal
import scala.Some
import spray.routing._
import org.apache.commons.lang.StringUtils
import com.intel.intelanalytics.engine.Engine
import com.intel.event.EventLogging

/**
 * Uses authorization HTTP header and engine to authenticate a user
 */
class AuthenticationDirective(val engine: Engine) extends Directives with EventLogging {

  /**
   * Gets authorization header and authenticates a user
   * @return the authenticated user
   */
  def authenticateKey: Directive1[UserPrincipal] =
    //TODO: proper authorization with spray authenticate directive in a manner similar to S3.
    optionalHeaderValue(getUserPrincipalFromHeader).flatMap {
      case Some(p) => provide(p)
      case None => reject(AuthenticationFailedRejection(AuthenticationFailedRejection.CredentialsMissing, List()))
    }

  protected def getUserPrincipalFromHeader(header: HttpHeader): Option[UserPrincipal] =
    condOpt(header) {
      case h if h.is("authorization") => Await.result(getUserPrincipal(h.value), ApiServiceConfig.defaultTimeout)
    }

  protected def getUserPrincipal(apiKey: String): Future[UserPrincipal] = withContext("AuthenticationDirective") {
    if (StringUtils.isBlank(apiKey)) {
      warn("Api key was not provided")
      throw new SecurityException("Api key was not provided")
    }
    future {
      val userPrincipal = engine.getUserPrincipal(apiKey)
      info("authenticated " + userPrincipal)
      userPrincipal
    }
  }
}
