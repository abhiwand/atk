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

import com.intel.intelanalytics.domain.User
import com.intel.intelanalytics.repository.MetaStore
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.shared.EventLogging
import spray.http.HttpHeader
import spray.routing._

import scala.PartialFunction._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

/**
 * Uses authorization HTTP header and metaStore to authenticate a user
 */
class AuthenticationDirective(val metaStore: MetaStore) extends Directives with EventLogging {

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

  protected def getUserPrincipal(apiKey: String): Future[UserPrincipal] = {
    future {
      metaStore.withSession("Getting user principal") { implicit session =>
        val users: List[User] = metaStore.userRepo.retrieveByColumnValue("api_key", apiKey)
        users match {
          case Nil => throw new SecurityException("User not found")
          case us if us.length > 1 => throw new SecurityException("Problem accessing user credentials")
          case user => {
            val userPrincipal: UserPrincipal = new UserPrincipal(users(0), List("user")) //TODO need role definitions
            info("Authenticated user " + userPrincipal)
            userPrincipal
          }
        }
      }
    }
  }
}
