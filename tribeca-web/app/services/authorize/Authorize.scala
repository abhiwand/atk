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

package services.authorize

import play.api.libs.json.JsValue
import services.authorize.providers.google.{GoogleTokenResponse, GooglePlus}
import services.authorize.Providers.Providers

class Authorize(var authData: JsValue, var provider: Providers.Providers) {

    var Provider = provider
    var jsonData = authData
    var responseData: Option[TokenResponse] = None
    var userInfo: Option[UserInfo] = None;

    /**
     *
     * @return a flag indicate whether the token response is valid
     */
    def validateTokenResponseData(): Boolean = {
        Provider match {
            case Providers.GooglePlus =>
                responseData = GooglePlus.validateTokenResponseData(jsonData)
                return if (responseData != None) true else false
            case Providers.None =>
                return false;

        }
    }

    def validateToken(): Option[UserInfo] = {
        Provider match {
            case Providers.GooglePlus =>
                userInfo = GooglePlus.validateToken(jsonData)
                if (userInfo != None && userInfo.get.email != null) userInfo else None
            case Providers.None =>
                None
        }
    }

    def validateUserInfo(): Option[UserInfo] = {
        Provider match {
            case Providers.GooglePlus =>
                userInfo = GooglePlus.validateUserInfo(authData)
                userInfo
            case _ =>
                None
        }

    }

    def getUserInfo(): Option[UserInfo] = {
        Provider match {
            case Providers.GooglePlus =>
                if(responseData == None)
                    None

                userInfo = GooglePlus.getUserInfo(responseData.get.access_token)
                userInfo
            case _ =>
                None
        }
    }

    def isAuthResponseDataValid(): Boolean = {
        (validateTokenResponseData() && validateToken() != None && getUserInfo() != None)
    }

    def getJavascriptOauthParams( provider: Providers ): String = {
      provider match{
        case Providers.GooglePlus =>
          GooglePlus.getJavascriptOauthParams()
      }
    }
}
