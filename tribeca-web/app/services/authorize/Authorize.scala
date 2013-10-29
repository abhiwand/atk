package services.authorize

import play.api.libs.json.JsValue
import services.authorize.providers.google.{GoogleTokenResponse, GooglePlus}

class Authorize(var authData: JsValue, var provider:Providers.Providers ) {

  var Provider = provider
  var jsonData = authData
  var responseData: TokenResponse = _;

  def valdiateTokenResponseData(): Boolean = {
    Provider match{
      case Providers.GooglePlus =>
        responseData = GooglePlus.validateTokenResponseData(jsonData)
        return if(responseData != null) true else false
      case Providers.None =>
        return false;

    }
  }

  def validateToken(): Boolean = {
    Provider match{
      case Providers.GooglePlus =>
       var bb = GooglePlus.validateToken(responseData.access_token)
        return true;
      case Providers.None =>
        return false;

    }
  }

  def getUserInfo(): UserInfo = {
    Provider match{
      case Providers.GooglePlus =>
        val userinfo = GooglePlus.getUserInfo(responseData.access_token)
        return userinfo
      case _ =>
        return null
    }
  }

    def isAuthResponseDataValid(): Boolean = {
        (valdiateTokenResponseData() && validateToken() && getUserInfo() != null)
    }
}
