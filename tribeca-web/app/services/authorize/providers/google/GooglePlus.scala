//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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

package services.authorize.providers.google


import play.api.libs.json._
import scala.concurrent.duration._
import play.api.libs.ws.WS
import scala.concurrent.Await
import play.api.libs.concurrent.Execution.Implicits._
import services.authorize.{UserInfo, TokenResponse}

/**
 * Singleton object to provide google oauth services.
 */
object GooglePlus {
    val https443_clientId = "141308260505-3qf2ofckirolrkajt3ansibkuk5qug5t.apps.googleusercontent.com"
    val https_clientId = "141308260505-jf332k2mi49jggi2cugf08vk17u9s9rk.apps.googleusercontent.com"
    val http_clientId = "141308260505-jf332k2mi49jggi2cugf08vk17u9s9rk.apps.googleusercontent.com"
    val clientSecret = "0fp9P9isYAz_vrlyA9I1Jk_j"
    val tokenVerifyUrl = "https://www.googleapis.com/oauth2/v1/tokeninfo"
    val userInfoUrl = "https://www.googleapis.com/oauth2/v1/userinfo"

    implicit val validateTokenResponseData = Json.reads[ValidateTokenResponseData]
    implicit val validateTokenJson = Json.reads[ValidateTokenJson]
    implicit val validateUserInfo = Json.reads[GoogleUserInfo]

    def validateClientId(idToValidate: String): Boolean = {
        if (idToValidate == https_clientId || idToValidate == http_clientId || idToValidate == https443_clientId) {
            true
        } else {
            false
        }
    }

    def validateTokenResponseData(authData: JsValue): Option[TokenResponse] = {
        authData.validate[ValidateTokenResponseData](validateTokenResponseData).map {
            case (validResponse) =>
                if (validateClientId(validResponse.client_id)) {
                    return Some(new GoogleTokenResponse(validResponse.access_token, validResponse.authuser, validResponse.client_id))
                }
        }.recoverTotal {
            return None
        }
        return None
    }

    def validateToken(token: String): Option[UserInfo] = {
        val responseFuture = WS.url(tokenVerifyUrl).withQueryString("access_token" -> token).get()
        val resultFuture = responseFuture map {
            response =>
                response.status match {
                    case 200 =>
                        Json.parse(response.body).validate[ValidateTokenJson](validateTokenJson).map {
                            case (validateTokenJson) =>
                                validateTokenJson
                        }
                    case _ =>
                        return None
                }
        }

        //this makes it a synchronous request
        val result = Await.result(resultFuture, 30 seconds)
        if (validateClientId(result.get.audience)) {
            return Some(UserInfo("", result.get.email, "", ""))
        } else {
            return None
        }
    }

    def validateUserInfo(body: JsValue): Option[UserInfo] = {

        body.validate[GoogleUserInfo](validateUserInfo).map {
            case (validUser) =>
                return Some(UserInfo(validUser.id, validUser.email, validUser.given_name, validUser.family_name))
        }
        None
    }

    def getUserInfo(token: String): Option[UserInfo] = {
        val responseFuture = WS.url(userInfoUrl).withQueryString("access_token" -> token).get()
        val resultFuture = responseFuture map {
            response =>
                response.status match {
                    case 200 => {
                        validateUserInfo(Json.parse(response.body))
                    }
                    case _ =>
                        None
                }
        }

        //this makes it a synchronous request
        val result = Await.result(resultFuture, 30 seconds)
        return Some(UserInfo(result.get.id, result.get.email, result.get.givenName, result.get.familyName))
    }
}