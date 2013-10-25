package controllers

import play.api.libs.json._


object AuthController {

  def getToken(jsonData : String) : String = {
    val json : JsValue = Json.parse(jsonData)
    val token = {
      json \ "access_token"
    }

    token.toString().drop(1).dropRight(1)
  }

  def getDataFromGoogleByToken(token : String) : Map[String, String] = {
     null
  }

  def getUserDataFromGoogle(jsonData : String) : Map[String, String] = {
    getDataFromGoogleByToken(getToken(jsonData))
  }
}
