package services.authorize.providers.google


import play.api.mvc._
import play.api.libs.json._
import com.fasterxml.jackson.databind.JsonNode
import play.api.libs.functional.syntax._
import scala.concurrent.Await
import scala.concurrent.duration._
import play.api.libs.ws.WS
import scala.concurrent.Await
import play.api.libs.concurrent.Execution.Implicits._
import services.authorize.{UserInfo, TokenResponse}
import services.authorize.providers.google.ValidateTokenJson


import play.api.mvc.Controller

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
    if(idToValidate == https_clientId || idToValidate == http_clientId || idToValidate == https443_clientId){
      return true
    }else{
      return false
    }
  }
  def validateTokenResponseData(authData: JsValue): TokenResponse = {
    authData.validate[ValidateTokenResponseData](validateTokenResponseData).map{
      case(validResponse) =>
        if(validateClientId(validResponse.client_id)){
          return new GoogleTokenResponse(validResponse.access_token, validResponse.authuser, validResponse.client_id)
        }
    }.recoverTotal{
      return null
    }
    return null
  }

  def validateToken(token: String): UserInfo = {
    val responseFuture = WS.url(tokenVerifyUrl).withQueryString("access_token"-> token).get()
    val resultFuture = responseFuture map{ response =>
      response.status match{
        case 200 =>
          Json.parse(response.body).validate[ValidateTokenJson](validateTokenJson).map{
            case(validateTokenJson) =>
            validateTokenJson
          }
        case _ =>
          return null
      }
    }

    //this makes it a synchronous request
    val result = Await.result(resultFuture, 30 seconds)
    if(validateClientId(result.get.audience)){
      return UserInfo("", result.get.email, "", "")
    } else {
      return null
    }
  }

  def validateUserInfo(body: JsValue): UserInfo = {
      body.validate[GoogleUserInfo](validateUserInfo).map{
        case(validUser) =>
          return UserInfo(validUser.id, validUser.email, validUser.given_name, validUser.family_name)
        case _ =>
          return null
    }
    null
  }

  def getUserInfo(token: String): UserInfo = {
    val responseFuture = WS.url(userInfoUrl).withQueryString("access_token"-> token).get()
    val resultFuture = responseFuture map{ response =>
      response.status match{
        case 200 =>{
          validateUserInfo(Json.parse(response.body))
        }
        case _ =>
          null
      }
    }

    //this makes it a synchronous request
    val result = Await.result(resultFuture, 30 seconds)
    result.email
    return UserInfo(result.id, result.email, result.givenName, result.familyName)
  }
}