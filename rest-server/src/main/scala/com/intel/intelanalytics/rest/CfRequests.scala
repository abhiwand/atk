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

import com.intel.event.EventLogging
import com.intel.intelanalytics.EventLoggingImplicits
import com.intel.intelanalytics.rest.RestServerConfig
import org.apache.commons.httpclient.HttpsURL
import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{ CloseableHttpResponse, HttpGet }
import org.springframework.security.jwt.JwtHelper
import org.springframework.security.jwt.crypto.sign.RsaVerifier
import org.apache.http.impl.client.HttpClients
import spray.json._
import DefaultJsonProtocol._

/**
 * Handles requests to Cloud Foundry servers
 */
object CfRequests extends EventLogging with EventLoggingImplicits {

  /**
   * User information extracted from a decoded token
   * @param userId
   * @param userName
   */
  case class TokenUserInfo(userId: String, userName: String)

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

  /**
   * Public key used to decode tokens coming in from clients
   */
  private lazy val oauthTokenPublicKey: String = {
    info(s"Asking UAA for the public token key")
    val ccServerUri = RestServerConfig.uaaUri
    val queryString = s"/token_key"
    val jsonResponse = httpsGetQuery(ccServerUri, queryString, headers = List.empty)
    try {
      jsonResponse.convertTo[UaaTokenPublicKeyResponse].value
    }
    catch {
      case ex: Throwable => throw new RuntimeException("Unexpected response format returned by UAA Server")
    }
  }

  /**
   * Verifier object used to validate the client tokens
   */
  private lazy val oauthTokenRsaVerifier: RsaVerifier = new RsaVerifier(oauthTokenPublicKey)

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
    info(s"Asking CC for the organizations of user $userId")
    val ccServerUri = RestServerConfig.ccUri
    val queryString = s"/v2/users/$userId/organizations"
    val headers = List(("Authorization", s"Bearer $token"))
    val jsonResponse = httpsGetQuery(ccServerUri, queryString, headers = headers)
    parseOrgIdsFromResponse(jsonResponse, "user")
  }

  /**
   * Asks the Cloud Controller for organization information filtered by the given spaceId
   * @param token
   * @param spaceId
   * @return the guid of the space's org
   */
  def getOrganizationForSpaceId(token: String, spaceId: String): String = {
    info(s"Asking CC for the organization of space $spaceId")
    val ccServerUri = RestServerConfig.ccUri
    val queryString = s"/v2/organizations?q=space_guid%3A$spaceId"
    val headers = List(("Authorization", s"Bearer $token"))
    val jsonResponse = httpsGetQuery(ccServerUri, queryString, headers)
    parseOrgIdsFromResponse(jsonResponse, "space")(0)
  }

  /**
   * Given a JsonResponse, parse and return the list of guids
   * @param jsonResponse jsonResponse obtained from a previous request
   * @param resource adds resource info to exception message
   * @return List of Organization Ids
   */
  def parseOrgIdsFromResponse(jsonResponse: JsValue, resource: String): List[String] = {
    jsonResponse.asJsObject.getFields("resources")(0) match {
      case JsArray(resources) => resources.map(r => r.asJsObject.getFields("metadata")(0)
        .asJsObject.getFields("guid")(0).asInstanceOf[JsString].value)
      case _ =>
        throw new RuntimeException(s"Unexpected response format returned by CC while fetching organizations for $resource id")
    }
  }

  /**
   * Makes a http GET request and returns the response as a JsValue
   * @param host target host url
   * @param queryString query string for GET
   * @param headers headers for GET request such as OAuth Authorization etc.
   * @return Parsed Json Response
   */
  private def httpsGetQuery(host: String, queryString: String, headers: List[(String, String)]): JsValue = withContext("httpsGetQuery") {

    // TODO: This method uses Apache HttpComponents HttpClient as spray-http library does not support proxy over https
    val (uri, port, scheme) = (host, HttpsURL.DEFAULT_PORT, new String(HttpsURL.DEFAULT_SCHEME))
    val (proxyHostConfigString, proxyPortConfigString) = ("https.proxyHost", "https.proxyPort")
    val httpClient = HttpClients.createDefault()
    try {
      val target = new HttpHost(uri, port, scheme)
      val proxy = (sys.props.contains(proxyHostConfigString), sys.props.contains(proxyPortConfigString)) match {
        case (true, true) => Some(new HttpHost(sys.props.get(proxyHostConfigString).get, sys.props.get(proxyPortConfigString).get.toInt))
        case _ => None
      }

      val config = {
        val cfg = RequestConfig.custom().setConnectTimeout(RestServerConfig.defaultTimeout.toSeconds.toInt)
        if (proxy.isDefined)
          cfg.setProxy(proxy.get).build()
        else cfg.build()
      }

      val request = new HttpGet(queryString)
      for ((headerTag, headerData) <- headers)
        request.addHeader(headerTag, headerData)
      request.setConfig(config)

      info("Executing request " + request.getRequestLine() + " to " + target + " via " + proxy.getOrElse("No proxy"))

      var response: Option[CloseableHttpResponse] = None
      try {
        response = Some(httpClient.execute(target, request))
        val inputStream = response.get.getEntity().getContent
        scala.io.Source.fromInputStream(inputStream).getLines().mkString("\n").parseJson
      }
      catch {
        case ex: Throwable =>
          error(s"Error executing request ${ex.getMessage}")
          throw new RuntimeException(s"Error connecting to $uri")
      }
      finally {
        if (response.isDefined)
          response.get.close()
      }
    }
    finally {
      httpClient.close()
    }
  }(null)

  // here's a canned key (for debugging purposes)
  //"-----BEGIN PUBLIC KEY-----\nMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDHFr+KICms+tuT1OXJwhCUmR2d\nKVy7psa8xzElSyzqx7oJyfJ1JZyOzToj9T5SfTIq396agbHJWVfYphNahvZ/7uMX\nqHxf+ZH9BL1gk9Y6kCnbM5R60gfwjyW1/dQPjOzn9N394zd2FJoFHwdq9Qs0wBug\nspULZVNRxq7veq/fzwIDAQAB\n-----END PUBLIC KEY-----\n"

}