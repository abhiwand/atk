package services.authorize

import play.api.libs.json.JsValue
import services.authorize.providers.google.{GoogleTokenResponse, GooglePlus}

class Authorize(var authData: JsValue, var provider:Providers.Providers ) {

  var Provider = provider
  var jsonData = authData
  var responseData: TokenResponse = _;
  var userInfo: UserInfo = _;

  def valdiateTokenResponseData(): Boolean = {
    Provider match{
      case Providers.GooglePlus =>
        responseData = GooglePlus.validateTokenResponseData(jsonData)
        return if(responseData != null) true else false
      case Providers.None =>
        return false;

    }
  }

  def validateToken(): UserInfo = {
    Provider match{
      case Providers.GooglePlus =>
       userInfo = GooglePlus.validateToken(responseData.access_token)
        if(userInfo != null && userInfo.email != null) userInfo else null
      case Providers.None =>
        null
    }
  }

  def getUserInfo(): UserInfo = {
    Provider match{
      case Providers.GooglePlus =>
        userInfo = GooglePlus.getUserInfo(responseData.access_token)
        return userInfo
      case _ =>
        return null
    }
  }

    def isAuthResponseDataValid(): Boolean = {
        (valdiateTokenResponseData() && validateToken() != null && getUserInfo() != null)
    }
}
