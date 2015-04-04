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

import akka.actor.ActorSystem
import spray.http._
import spray.json._
import spray.httpx.SprayJsonSupport._
import spray.client.pipelining._
import scala.concurrent.{ Await, Future }
import org.springframework.security.jwt.JwtHelper
import org.springframework.security.jwt.crypto.sign.RsaVerifier
import scala.util.{ Try, Failure, Success }

/**
 * Handles requests to Cloud Foundry servers
 */
object CfRequests {

  /**
   * User information extracted from a decoded token
   * @param userId
   * @param userName
   */
  case class TokenUserInfo(userId: String, userName: String)

  /**
   * Public key used to decode tokens coming in from clients
   */
  private lazy val oauthTokenPublicKey: String = getUaaTokenPublicKey

  /**
   * Verifier object used to validate the client tokens
   */
  private lazy val oauthTokenRsaVerifier: RsaVerifier = new RsaVerifier(oauthTokenPublicKey)

  /**
   * Represents the payload of the response from UAA when asking for token public key
   * @param alg
   * @param value
   */
  private case class UaaTokenPublicKeyResponse(alg: String, value: String)

  private object OauthJsonProtocol extends DefaultJsonProtocol {
    implicit val tokenPublicKeyResponseFormat = jsonFormat2(UaaTokenPublicKeyResponse)
  }
  import OauthJsonProtocol._

  implicit val system = ActorSystem()

  import system.dispatcher

  /**
   * Validates and decodes the token to return the embedded user info
   * @param token
   * @return
   */
  def getTokenUserInfo(token: String): TokenUserInfo = {
    val decodedKey = JwtHelper.decode(token)
    decodedKey.verifySignature(oauthTokenRsaVerifier)
    decodedKey.getClaims.parseJson.asJsObject().getFields("user_id", "user_name") match {
      case Seq(JsString(userId), JsString(userName)) => TokenUserInfo(userId = userId, userName = userName)
      case _ => throw new RuntimeException("Unexpected decode format for oauth token.  Could not find user_id and/or user_name")
    }
  }

  /**
   * Asks the Cloud Controller for information about the userId and extracts all the organizations it belongs to
   * @param token
   * @param userId
   * @return
   */
  def getOrganizationsForUserId(token: String, userId: String): List[String] = {
    //println(s"Asking CC for the organizations of user $userId")
    val uri = ApiServiceConfig.ccUri
    val url = s"$uri/v2/users/$userId/organizations"
    requestResourceIds(token, url)
  }

  /**
   * Asks the Cloud Controller for organization information filtered by the given spaceId
   * @param token
   * @param spaceId
   * @return the guid of the space's org
   */
  def getOrganizationForSpaceId(token: String, spaceId: String): String = {
    //println(s"Asking CC for the organization of space $spaceId")
    val uri = ApiServiceConfig.ccUri
    val url = s"$uri/v2/organizations?q=space_guid%3A$spaceId"
    requestResourceIds(token, url)(0)
  }

  /**
   * Calls CC URL and parses the response for the guids of the resources returned
   * @param token
   * @param url
   * @return
   */
  private def requestResourceIds(token: String, url: String): List[String] = {

    val pipeline: HttpRequest => Future[HttpResponse] = (
      addHeader("Accept", "application/json")
      ~> addCredentials(OAuth2BearerToken(token))
      ~> sendReceive
    )

    val response: HttpResponse = Await.result(pipeline(Get(url)), ApiServiceConfig.defaultTimeout)
    if (response.status.isFailure) {
      throw new RuntimeException(s"Unable to acquire user's organization from CC: $response")
    }
    val responseAst: JsValue = response.entity.data.asString.parseJson
    Try {
      responseAst.asJsObject.getFields("resources")(0) match {
        case JsArray(resources) => resources.map(r => r.asJsObject.getFields("metadata")(0).asJsObject.getFields("guid")(0).asInstanceOf[JsString].value)
        case _ => throw new RuntimeException("Expected array of resources")
      }
    } match {
      case Success(orgs) => orgs
      case Failure(ex) => throw new RuntimeException(s"Unexpected format returned from CC for organizations: $ex")
    }
  }

  /**
   * Get the Public Key from the UAA server for decoding client tokens
   * @return
   */
  private def getUaaTokenPublicKey: String = {

    val uri = ApiServiceConfig.uaaUri

    val pipeline: HttpRequest => Future[UaaTokenPublicKeyResponse] = (
      addHeader("Accept", "application/json")
      ~> sendReceive
      ~> unmarshal[UaaTokenPublicKeyResponse]
    )
    val response: Future[UaaTokenPublicKeyResponse] =
      pipeline(Get(uri + "/token_key"))

    Await.result(response, ApiServiceConfig.defaultTimeout).value

    // here's a canned key
    //"-----BEGIN PUBLIC KEY-----\nMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDHFr+KICms+tuT1OXJwhCUmR2d\nKVy7psa8xzElSyzqx7oJyfJ1JZyOzToj9T5SfTIq396agbHJWVfYphNahvZ/7uMX\nqHxf+ZH9BL1gk9Y6kCnbM5R60gfwjyW1/dQPjOzn9N394zd2FJoFHwdq9Qs0wBug\nspULZVNRxq7veq/fzwIDAQAB\n-----END PUBLIC KEY-----\n"
  }
}