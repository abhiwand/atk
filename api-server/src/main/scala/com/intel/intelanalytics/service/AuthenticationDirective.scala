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

package com.intel.intelanalytics.service

//TODO: Is this right execution context for us?

import java.nio.CharBuffer

import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.engine.plugin.{ Invocation, Call }
import org.apache.commons.codec.binary.Base64
import org.bouncycastle.operator.bc.BcRSAContentVerifierProviderBuilder
import org.springframework.security.jwt._
import org.springframework.security.jwt.crypto.sign.RsaVerifier
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
import java.security._

import scala.util.parsing.json.JSON

/**
 * Uses authorization HTTP header and engine to authenticate a user
 */
class AuthenticationDirective(val engine: Engine) extends Directives with EventLogging with EventLoggingImplicits {

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
      case h if h.is("authorization") => Await.result(getUserPrincipal(h.value)(Call(null)), ApiServiceConfig.defaultTimeout)
    }

  protected def getUserPrincipal(apiKey: String)(implicit invocation: Invocation): Future[UserPrincipal] = {
    withContext("AuthenticationDirective") {
      if (StringUtils.isBlank(apiKey)) {
        warn("Api key was not provided")
        throw new SecurityException("Api key was not provided")
      }
      future {
        val userPrincipal = engine.getUserPrincipal(apiKey)
        info("authenticated " + userPrincipal)

        validateUaaUser(apiKey)

        userPrincipal
      }
    }
  }

  protected def validateUaaUser(apiKey: String): Unit = {

    //val apiKey: String = "eyJhbGciOiJSUzI1NiJ9.eyJqdGkiOiI4OTUxNjZjNC1hYTgxLTRjZTUtYmM1OS05OTQ0NWVhNTQ3Y2QiLCJzdWIiOiIyNWFiMDgwYy1jMjI4LTRjZDktODg2YS1jZGY1YWQ0Nzg5M2MiLCJzY29wZSI6WyJjbG91ZF9jb250cm9sbGVyLndyaXRlIiwiY2xvdWRfY29udHJvbGxlcl9zZXJ2aWNlX3Blcm1pc3Npb25zLnJlYWQiLCJvcGVuaWQiLCJjbG91ZF9jb250cm9sbGVyLnJlYWQiXSwiY2xpZW50X2lkIjoiYXRrLWNsaWVudCIsImNpZCI6ImF0ay1jbGllbnQiLCJhenAiOiJhdGstY2xpZW50IiwiZ3JhbnRfdHlwZSI6InBhc3N3b3JkIiwidXNlcl9pZCI6IjI1YWIwODBjLWMyMjgtNGNkOS04ODZhLWNkZjVhZDQ3ODkzYyIsInVzZXJfbmFtZSI6ImFkbWluIiwiZW1haWwiOiJhZG1pbiIsImlhdCI6MTQyNjc5Mzk1NiwiZXhwIjoxNDI2ODM3MTU2LCJpc3MiOiJodHRwczovL3VhYS5nb3RhcGFhcy5jb20vb2F1dGgvdG9rZW4iLCJhdWQiOlsiYXRrLWNsaWVudCIsImNsb3VkX2NvbnRyb2xsZXIiLCJjbG91ZF9jb250cm9sbGVyX3NlcnZpY2VfcGVybWlzc2lvbnMiLCJvcGVuaWQiXX0.xJ5IlPsrHXutETZgdbTtiDTkliyIs26UYf_2oP-cwRWuxdsxAcphOXZEgHZXgNECr591Ts9R1-v8e6EihwW5x5CQ_7_BzmIYM0Z2IfYm220ZPktDkEryoKjujG5eqhUqjVGnj1og1ro6HX7ANu-HAXPhZ-USKu2eh_hR02EeZUU"
    val firstPeriod: Int = apiKey.indexOf('.')
    val lastPeriod: Int = apiKey.lastIndexOf('.')

    println(apiKey)
    println()

    val decodedKey = JwtHelper.decode(apiKey)
    println("decodedkey: " + decodedKey.toString()) \
      //val json = JSON // decodedKey.getClaims()
      println("claims: " + decodedKey.getClaims())
    println()

    val rsa = new RsaVerifier("-----BEGIN PUBLIC KEY-----\nMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDHFr+KICms+tuT1OXJwhCUmR2d\nKVy7psa8xzElSyzqx7oJyfJ1JZyOzToj9T5SfTIq396agbHJWVfYphNahvZ/7uMX\nqHxf+ZH9BL1gk9Y6kCnbM5R60gfwjyW1/dQPjOzn9N394zd2FJoFHwdq9Qs0wBug\nspULZVNRxq7veq/fzwIDAQAB\n-----END PUBLIC KEY-----\n")
    val jwt = decodedKey.verifySignature(rsa)

  }

}
