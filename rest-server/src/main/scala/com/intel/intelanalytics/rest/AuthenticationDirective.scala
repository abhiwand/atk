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

package com.intel.intelanalytics.rest

//TODO: Is this right execution context for us?

import java.util.concurrent.{ Callable, TimeUnit }

import com.google.common.cache.CacheBuilder
import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.engine.plugin.{ Invocation, Call }
import org.springframework.security.jwt._
import org.springframework.security.jwt.crypto.sign.RsaVerifier
import spray.http.HttpHeader
import spray.client

import scala.PartialFunction._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import com.intel.intelanalytics.security.UserPrincipal
import spray.routing._
import org.apache.commons.lang.StringUtils
import com.intel.intelanalytics.engine.Engine
import com.intel.event.EventLogging
import org.joda.time.DateTime

import scala.util.parsing.json.JSON
import scala.util.{ Failure, Success, Try }
import com.intel.intelanalytics.rest.CfRequests.TokenUserInfo

/**
 * Uses authorization HTTP header and engine to authenticate a user
 */
class AuthenticationDirective(val engine: Engine) extends Directives with EventLogging with EventLoggingImplicits {

  private lazy val shortCircuitApiKey = RestServerConfig.shortCircuitApiKey

  /**
   * Caches user principals so that they don't have to be looked up every time.
   *
   * (This was originally added for QA parallel testing)
   */
  private lazy val cache = CacheBuilder.newBuilder()
    .expireAfterWrite(RestServerConfig.userPrincipalCacheTimeoutSeconds, TimeUnit.SECONDS)
    .maximumSize(RestServerConfig.userPrincipalCacheMaxSize)
    .build[String, UserPrincipal]()

  /**
   * Gets authorization header and authenticates a user
   * @return the authenticated user
   */
  def authenticateKey: Directive1[Invocation] =
    //TODO: proper authorization with spray authenticate directive in a manner similar to S3.
    optionalHeaderValue(getUserPrincipalFromHeader).flatMap {
      case Some(p) => provide(Call(p))
      case None => reject(AuthenticationFailedRejection(AuthenticationFailedRejection.CredentialsMissing, List()))
    }

  protected def getUserPrincipalFromHeader(header: HttpHeader): Option[UserPrincipal] =
    condOpt(header) {
      case h if h.is("authorization") => getUserPrincipal(h.value)
    }

  protected def getUserPrincipal(apiKey: String): UserPrincipal = {
    cache.get(apiKey, new Callable[UserPrincipal]() {
      override def call(): UserPrincipal = {
        // cache miss, look it up
        Await.result(lookupUserPrincipal(apiKey)(Call(null)), RestServerConfig.defaultTimeout)
      }
    })
  }

  protected def lookupUserPrincipal(apiKey: String)(implicit invocation: Invocation): Future[UserPrincipal] = {
    withContext("AuthenticationDirective") {
      if (StringUtils.isBlank(apiKey)) {
        warn("Api key was not provided")
        throw new SecurityException("Api key was not provided")
      }
      future {
        val tokenUserInfo = if (apiKey.equals(shortCircuitApiKey)) {
          TokenUserInfo(userId = shortCircuitApiKey, userName = shortCircuitApiKey)
        }
        else {
          CfRequests.getTokenUserInfo(apiKey)
        }

        // todo - add mapping support from userId to userName for humans
        val userKey = tokenUserInfo.userId
        val userPrincipal: UserPrincipal = Try { engine.getUserPrincipal(userKey) } match {
          case Success(found) => found
          case Failure(missing) =>
            // Don't know about this user id.  See if the user meets requirements to be added to the metastore
            // 1. The userId must belong to the same organization as this server instance
            val userOrganizationIds = CfRequests.getOrganizationsForUserId(apiKey, tokenUserInfo.userId)
            val appOrganizationId = CfRequests.getOrganizationForSpaceId(apiKey, RestServerConfig.appSpace)
            if (userOrganizationIds.contains(appOrganizationId)) {
              engine.addUserPrincipal(userKey)
            }
            else {
              throw new RuntimeException(s"User ${tokenUserInfo.userId} (${tokenUserInfo.userName}) is not a member of this server's organization")
            }
        }
        info("authenticated " + userPrincipal)
        userPrincipal
      }
    }
  }

}
